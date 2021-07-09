%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_retainer_storage).
-compile(inline).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ handle_connect/2, begin_read/2, handle_dispatch/4
        , handle_insert/1, handle_close/0, modify_table_size/1
        , handle_read/3, get_table_size/0, reset_table_size/0
        , dispatch/4, continue_read/3, handle_clear_expired/0
        , handle_delete/1, reset_table_size/1, lock_context/1
        , reset_deliver_quota/0]).

-export([unsafe_delete/1, unsafe_clean/0, get_all_topics/0]).

-export([safe_call_backend/3]).

-export_type([open_args/0, operate_result/0, cursor/0]).

-type open_args() :: map().

-type context_key() :: backend
                     | table_size
                     | read_remained_quota
                     | deliver_remained_quota
                     | is_locked.

-type semaphore() :: ?DELIVER_SEMAPHORE.

-type cursor() :: undefined | term().
-type result() :: term().
-type backend() :: emqx_retainer_storage_mnesia.

-type operate_result() :: ok
                        | abort.

-type read_result() :: {ok, result(), cursor()}.

-type replace_result() :: operate_result()
                        | table_full.

-callback open(open_args()) -> operate_result().
-callback close() -> operate_result().
-callback insert(message()) -> operate_result().
-callback replace(message()) -> replace_result().
-callback clear_expired() -> operate_result().
-callback delete(topic()) -> operate_result().
-callback clean() -> operate_result().
-callback read(topic(), cursor()) -> read_result().
-callback load_table_size() -> non_neg_integer().
-callback get_all_topics() -> list(topic()).

-define(DEF_MAX_RETAINED_MESSAGES, 0).
-define(SAFE_CALL(API, Paramters),
        {?MODULE, safe_call_backend, [0, API, Paramters]}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
-spec handle_connect(emqx_retaienr_schema:storage_type(), open_args()) -> operate_result().
handle_connect(StorageType, OpenArgs) ->
    Backend = get_backend_module(StorageType),
    ok = init_context(Backend),
    Backend:open(OpenArgs).

-spec begin_read(pid(), topic()) -> operate_result().
begin_read(Pid, Topic) ->
    continue_read(Pid, Topic, undefined).

-spec continue_read(pid(), topic(), cursor()) -> operate_result().
continue_read(Pid, Topic, Cursor) ->
    case erlang:is_process_alive(Pid) of
        false ->
            ok;
        _ ->
            require_locker(context, {?MODULE, handle_read, [Pid, Topic, Cursor]})
    end.

-spec handle_read(pid(), topic(), cursor()) -> operate_result() | replace_result().
handle_read(Pid, Topic, Cursor) ->
    case safe_call_backend(0, read, [Topic, Cursor]) of
        {ok, Result, NewCursor} ->
            handle_dispatch(Pid, Topic, Result, NewCursor);
        _ ->
            ok
    end.

-spec handle_dispatch(pid(), topic(), result(), cursor()) -> operate_result().
handle_dispatch(Pid, Topic, Result, Cursor) ->
    try
        dispatch(Pid, Topic, Result, Cursor)
    catch _:Reason:Trace ->
            %%never redo dispatch, because no way to determine which has been successfully send
            ?ERROR("deliver failure, Reason:~p~n, Stack:~p~n", [Reason, Trace]),
            ok
    end.

-spec handle_insert(message()) -> replace_result().
handle_insert(#message{topic = Topic} = Msg) ->
    case is_table_full() of
        false ->
            require_locker(context, ?SAFE_CALL(insert, [Msg]));
        _ ->
            case require_locker(context, ?SAFE_CALL(replace, [Msg])) of
                table_full ->
                    ?LOG(error, "Cannot retain message(topic=~s) for table is full!", [Topic]),
                    ok;
                Any ->
                    Any
            end
    end.

-spec handle_close() -> ok.
handle_close() ->
    Backend = get_context(backend),
    ok = Backend:close(),
    true = ets:delete_all_objects(?CONTEXT_TAB),
    ok.

-spec handle_clear_expired() -> operate_result().
handle_clear_expired() ->
    require_locker(context, ?SAFE_CALL(clear_expired, [])).

-spec handle_delete(topic()) -> operate_result().
handle_delete(Topic) ->
    require_locker(context, ?SAFE_CALL(delete, [Topic])).

-spec get_table_size() -> non_neg_integer().
get_table_size() ->
    get_context(table_size).

-spec modify_table_size(integer()) -> non_neg_integer().
modify_table_size(Inc) ->
    ets:update_counter(?CONTEXT_TAB, table_size, {#context.value, Inc}).

-spec reset_table_size() -> ok.
reset_table_size() ->
    Backend = get_context(backend),
    insert_context(table_size, Backend:load_table_size()).

-spec reset_table_size(non_neg_integer()) -> ok.
reset_table_size(Value) ->
    insert_context(table_size, Value).

-spec lock_context(boolean()) -> ok.
lock_context(IsToLock) ->
    insert_context(is_locked, IsToLock).

-spec reset_deliver_quota() -> ok.
reset_deliver_quota() ->
    insert_context(?DELIVER_SEMAPHORE, get_deliver_quota()).

-spec dispatch(pid(), topic(), list(term()), cursor()) -> operate_result().
dispatch(Pid, Topic, Result, Cursor) ->
    case erlang:is_process_alive(Pid) of
        false ->
            ok;
        _ ->
            #{deliver_quota := MaxDeliverNum,
              deliver_release_interval := SleepTime} = emqx_config:get([?APP, flow_control]),
            case MaxDeliverNum of
                0 ->
                    _ = [Pid ! {deliver, Topic, Msg} || Msg <- Result],
                    check_has_continue_read(Pid, Topic, Cursor);
                _ ->
                    case do_dispatch(Result, Pid, Topic, SleepTime) of
                        ok ->
                            check_has_continue_read(Pid, Topic, Cursor);
                        Any ->
                            Any
                    end
            end
    end.

%%--------------------------------------------------------------------
%% Unsafe APIs
%%--------------------------------------------------------------------
-spec unsafe_delete(topic()) -> operate_result().
unsafe_delete(Topic) ->
    Backend = get_context(backend),
    Backend:delete(Topic).

-spec unsafe_clean() -> operate_result().
unsafe_clean() ->
    Backend = get_context(backend),
    Backend:clean().

-spec get_all_topics() -> list(topic()).
get_all_topics() ->
    Backend = get_context(backend),
    Backend:get_all_topics().

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec do_dispatch(list(term()), pid(), topic(), non_neg_integer()) ->
          operate_result().
do_dispatch([], _, _, _) ->
    ok;
do_dispatch([Msg | T], Pid, Topic, DeliverInterval) ->
    case require_semaphore(?DELIVER_SEMAPHORE) of
        ok ->
            Pid ! {deliver, Topic, Msg},
            do_dispatch(T,
                        Pid,
                        Topic,
                        DeliverInterval);
        Any ->
            Any
    end.

-spec check_has_continue_read(pid(), topic(), cursor()) -> operate_result().
check_has_continue_read(_, _, undefined) ->
    ok;
check_has_continue_read(Pid, Topic, Cursor) ->
    continue_read(Pid, Topic, Cursor).

-spec is_table_full() -> boolean().
is_table_full() ->
    Limit = emqx_config:get([?APP, size_control, max_retained_messages],
                            ?DEF_MAX_RETAINED_MESSAGES),
    Limit > 0 andalso (get_table_size() >= Limit).

-spec init_context(backend()) -> ok.
init_context(Backend) ->
    lists:foreach(fun({K, V}) ->
                          insert_context(K, V)
                  end,
                  [ {table_size, 0}
                  , {is_locked, false}
                  , {backend, Backend}
                  , {deliver_remained_quota, get_deliver_quota()}]).

-spec safe_call_backend(non_neg_integer(), atom(), list()) -> operate_result() | read_result().
safe_call_backend(RedoTimes, F, A) ->
    Backend = get_context(backend),
    try
        erlang:apply(Backend, F, A)
    catch Type:Reason:Stacktrace ->
            ?ERROR("processing action: {~p, ~p} failed, Reason: ~p~n, Stack: ~p.~nredo times:~p~n",
                   [Backend, F, Reason, Stacktrace, RedoTimes]),
            on_call_exception(Type, Reason, Stacktrace, RedoTimes , F, A)
    end.

-spec on_call_exception(atom(), atom(), term(), non_neg_integer(), atom(), list()) ->
          operate_result().
on_call_exception(Type, Reason, Stacktrace, RedoTimes, F, A) ->
    [Strategy | _] = emqx_config:get([?APP, action_failure_strategy]),
    case Strategy of
        log_only ->
            ok;
        throw ->
            erlang:raise(Type, Reason, Stacktrace);
        #{max_try_times := TryTimes,
          try_interval := TryInterval,
          failure_strategy := FailureStrategy} ->
            case RedoTimes >= TryTimes of
                true ->
                    case FailureStrategy of
                        log_only ->
                            ok;
                        _ ->
                            erlang:raise(Type, Reason, Stacktrace)
                    end;
                _ ->
                    case emqx_retainer:wait_time(TryInterval) of
                        ok ->
                            safe_call_backend(RedoTimes + 1, F, A);
                        abort ->
                            abort
                    end
            end
    end.

-spec require_locker(locker(), {module(), atom(), list()}) -> operate_result() | replace_result().
require_locker(Locker, {M, F, A}) ->
    case check_locker_require(Locker) of
        ok ->
            erlang:apply(M, F, A);
        abort ->
            abort
    end.

-spec check_locker_require(locker()) -> operate_result().
check_locker_require(context) ->
    IsLocked = get_context(is_locked),
    wait_locker(IsLocked, context).

-spec require_semaphore(semaphore()) -> operate_result().
require_semaphore(Semaphore) ->
    Remained = ets:update_counter(?CONTEXT_TAB,
                                  Semaphore,
                                  {#context.value, -1, 0, 0}),
    wait_locker(Remained =< 0, Semaphore).

-spec wait_locker(IsLocked :: boolean(), locker() | semaphore()) -> operate_result().
wait_locker(false, _) ->
    ok;
wait_locker(_, Name) ->
    emqx_retainer:wait_locker(Name).

-spec get_context(context_key()) -> term().
get_context(Key) ->
    [Cxt | _] = ets:lookup(?CONTEXT_TAB, Key),
    Cxt#context.value.

-spec insert_context(context_key(), term()) -> ok.
insert_context(Key, Term) ->
    ets:insert(?CONTEXT_TAB, #context{key = Key, value = Term}),
    ok.

-spec get_deliver_quota() -> non_neg_integer().
get_deliver_quota() ->
    emqx_config:get([?APP, flow_control, deliver_quota]).

-spec get_backend_module(emqx_retaienr_schema:storage_type()) -> backend().
get_backend_module(builtin) ->
    emqx_retainer_storage_mnesia.

