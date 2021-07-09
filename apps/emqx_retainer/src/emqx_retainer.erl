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

-module(emqx_retainer).
-compile(inline).

-behaviour(gen_statem).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[Retainer]").

-export([start_link/0]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([handle_event/4]).

-export([ on_session_subscribed/3
        , on_message_publish/1
        ]).

-export([ publish/1
        , delete/1
        , subscribed/2]).

-export([ get_table_size/0, clean/0, sync_delete/1
        , get_all_topics/0, update_config/1]).

-export([wait_time/1, wait_locker/1, safe_release_context/1]).

-define(DEF_STATS_INTERVAL, timer:seconds(1)).
-define(DEF_MAX_PAYLOAD_SIZE, (1024 * 1024)).
%% cast to me
-define(CAST(Msg), gen_statem:cast(?APP, Msg)).
-define(CALL(Req), gen_statem:call(?APP, Req)).

-record(data,
        { stats_fun
        , context_lockers :: list(gen_statem:from())
        , deliver_lockers :: list(gen_statem:from())
        , wait_timers :: sets:set(gen_statem:from())}).

-rlog_shard({?RETAINER_SHARD, ?TAB}).

-type data() :: #data{}.
-type state() :: disable
               | enable.

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

load() ->
    _ = emqx:hook('session.subscribed', {?MODULE, on_session_subscribed, []}),
    _ = emqx:hook('message.publish', {?MODULE, on_message_publish, []}),
    ok.

unload() ->
    emqx:unhook('message.publish', {?MODULE, on_message_publish}),
    emqx:unhook('session.subscribed', {?MODULE, on_session_subscribed}).

on_session_subscribed(_, _, #{share := ShareName}) when ShareName =/= undefined ->
    ok;
on_session_subscribed(_, Topic, #{rh := Rh, is_new := IsNew}) ->
    case Rh =:= 0 orelse (Rh =:= 1 andalso IsNew) of
        true ->
            emqx_pool:async_submit({?MODULE, subscribed, [self(), Topic]});
        _ -> ok
    end.

%% RETAIN flag set to 1 and payload containing zero bytes
on_message_publish(#message{flags = #{retain := true},
                            topic = Topic,
                            payload = Payload} = Msg) ->
    case Payload of
        <<>> ->
            emqx_pool:async_submit({?MODULE, delete, [Topic]});
        _ ->
            Msg1 = emqx_message:set_header(retained, true, Msg),
            emqx_pool:async_submit({?MODULE, publish, [Msg1]})
    end;
on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Start the retainer
-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> [handle_event_function, state_enter].

-spec publish(message()) -> ok.
publish(#message{topic = Topic, payload = Payload} = Msg) ->
    case is_too_big(erlang:byte_size(Payload)) of
        false ->
            emqx_retainer_storage:handle_insert(Msg);
        _ ->
            ?ERROR("Cannot retain message(topic=~s, payload_size=~p) for payload is too big!",
                   [Topic, iolist_size(Payload)])
    end.

-spec delete(topic()) -> ok.
delete(Topic) ->
    emqx_retainer_storage:handle_delete(Topic).

-spec subscribed(Pid :: pid(), Topic :: binary()) -> ok.
subscribed(Pid, Topic) ->
    emqx_retainer_storage:begin_read(Pid, Topic).

-spec get_table_size() -> non_neg_integer().
get_table_size() ->
    emqx_retainer_storage:get_table_size().

-spec clean() -> ok.
clean() ->
    emqx_pool:async_submit({emqx_retainer_storage, unsafe_clean, []}).

-spec sync_delete(topic()) -> ok.
sync_delete(Topic) ->
    emqx_retainer_storage:handle_delete(Topic).

-spec get_all_topics() -> list(binary()).
get_all_topics() ->
    emqx_retainer_storage:get_all_topics().

-spec update_config(hocon:config()) -> ok.
update_config(Conf) ->
    ?CALL({?FUNCTION_NAME, Conf}).

-spec wait_time(non_neg_integer()) -> ok | abort.
wait_time(Interval) ->
    ?CALL({?FUNCTION_NAME, Interval}).

-spec wait_locker(locker()) -> ok | abort.
wait_locker(Name) ->
    ?CALL({?FUNCTION_NAME, Name}).

-spec safe_release_context(atom()) -> ok.
safe_release_context(Result) ->
    emqx_retainer_storage:lock_context(false),
    ?CAST({release_context, Result}).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

init([]) ->
    Data = #data{ stats_fun = emqx_stats:statsfun('retained.count', 'retained.max')
                , context_lockers = []
                , deliver_lockers = []
                , wait_timers = sets:new()},

    Options = [ set, public, {read_concurrency, true}
              , {write_concurrency, true}, {keypos, #context.key}, named_table],
    ?CONTEXT_TAB = ets:new(?CONTEXT_TAB, Options),
    check_enable(Data).

handle_event(enter, _, disable, Data) ->
    unload(),
    disable(Data);

handle_event(_, _, disable, _) ->
    keep_state_and_data;

handle_event({call, From}, {wait_locker, context}, _, #data{context_lockers = Lockers} = Data) ->
    {keep_state, Data#data{context_lockers = [From | Lockers]}};

handle_event({call, From},
             {wait_locker, ?DELIVER_SEMAPHORE}, _, #data{deliver_lockers = Lockers} = Data) ->
    {keep_state, Data#data{deliver_lockers = [From | Lockers]}};

handle_event({call, From}, {wait_time, Interval}, _, #data{wait_timers = WaitTimers} = Data) ->
    {keep_state,
     Data#data{wait_timers = sets:add_element(From, WaitTimers)},
     {{timeout, {wait_time, From}}, Interval, wait_time}};

handle_event({call, From}, {update_config, Conf}, _, Data) ->
    update_config(Data, From, Conf);

handle_event(cast, {release_context, Result}, _, #data{context_lockers = Lockers} = Data) ->
    [gen_statem:reply(From, Result) || From <- lists:reverse(Lockers)],
    {keep_state,
     Data#data{context_lockers = []}};

handle_event({timeout, clear_expired}, _, _, _) ->
    emqx_retainer_storage:handle_clear_expired(),
    {keep_state_and_data, add_clear_timer([])};

handle_event({timeout, stats_timer}, _, _, #data{stats_fun = Fun}) ->
    Size = emqx_retainer_storage:get_table_size(),
    Fun(Size),
    {keep_state_and_data,
     {{timeout, stats_timer}, ?DEF_STATS_INTERVAL, stats}};

handle_event({timeout, release_deliver_lockers}, _, _, #data{deliver_lockers = Lockers} = Data) ->
    emqx_retainer_storage:reset_deliver_quota(),
    [gen_statem:reply(From, ok) || From <- lists:reverse(Lockers)],
    Data2 = Data#data{deliver_lockers = []},
    {keep_state, Data2, add_deliver_timer([])};

handle_event({timeout, {wait_time, From}}, _, _, #data{wait_timers = WaitTimers} = Data) ->
    gen_statem:reply(From, ok),
    {keep_state,
     Data#data{wait_timers = sets:del_element(From, WaitTimers)}};

handle_event(_, _, _, _) ->
    keep_state_and_data.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec disable(data()) -> gen_statem:event_handler_result(state()).
disable(#data{wait_timers = Timers} = Data) ->
    ok = emqx_retainer_storage:handle_close(),
    Data2 = reset_data(Data),
    {keep_state,
     Data2,
     sets:fold(fun(Timer, Acc) ->
                       [{{timeout, {wait_time, Timer}}, cancel} | Acc]
               end, 
               [ {{timeout, clear_expired}, cancel}
               , {{timeout, stats_timer}, cancel}
               , {{timeout, release_deliver_lockers}, cancel}
               ],
               Timers)}.

-spec connect(data(), atom()) -> gen_statem:event_handler_result(state())
              | gen_statem:action(state()).
connect(Data, Tag) ->
    #{storage_type := StorageType,
      connector := [Connector | _]} = emqx_config:get([?APP]),
    ok = emqx_retainer_storage:handle_connect(StorageType, Connector),
    load(),
    TimerActions = [{{timeout, stats_timer}, ?DEF_STATS_INTERVAL, stats}],
    {Tag,
     enable,
     Data,
     add_deliver_timer(add_clear_timer(TimerActions))}.

-spec is_too_big(non_neg_integer()) -> boolean().
is_too_big(Size) ->
    Limit = emqx_config:get([?MODULE, size_control, max_payload_size], ?DEF_MAX_PAYLOAD_SIZE),
    Limit > 0 andalso (Size > Limit).

-spec add_optional_timer(atom(),
                         undefined | non_neg_integer(),
                         list(gen_statem:action())) -> list(gen_statem:action()).
add_optional_timer(Timer, Interval, TransActions) ->
    case is_integer(Interval) andalso Interval > 0 of
        true ->
            [{{timeout, Timer}, Interval, Timer} | TransActions];
        _ ->
            TransActions
    end.

-spec add_clear_timer(list(gen_statem:action())) -> list(gen_statem:action()).
add_clear_timer(Actions) ->
    Interval = emqx_config:get([?APP, clear_interval]),
    add_optional_timer(clear_expired, Interval, Actions).

-spec add_deliver_timer(list(gen_statem:action())) -> list(gen_statem:action()).
add_deliver_timer(Actions) ->
    Interval = emqx_config:get([?APP, flow_control, deliver_release_interval]),
    add_optional_timer(release_deliver_lockers, Interval, Actions).

-spec check_enable(data()) -> gen_statem:init_result(state()).
check_enable(Data) ->
    Enable = emqx_config:get([?APP, enable]),
    case Enable of
        true ->
            connect(Data, ok);
        _ ->
            {ok, disable, Data}
    end.

-spec reset_data(data()) -> data().
reset_data(#data{context_lockers = CxtLockers,
                 deliver_lockers = DeliverLockers,
                 wait_timers = WaitTimers} = Data) ->
    [gen_statem:reply(From, abort) || From <- CxtLockers],
    [gen_statem:reply(From, abort) || From <- DeliverLockers],
    sets:fold(fun(From, _) ->
                      gen_statem:reply(From, abort),
                      ok
              end, ok, WaitTimers),
    Data#data{context_lockers = [],
              deliver_lockers = [],
              wait_timers = sets:new()}.

-spec update_config(data(), gen_statem:from(), hocons:config()) ->
          gen_statem:handle_event_result(state()).
update_config(Data, From, Conf) ->
    emqx_retainer_storage:lock_context(true),
    #{enable := Enable,
      storage_type := StorageType,
      flow_control := #{deliver_release_interval := Deliverval},
      clear_interval := ClearInterval} = Conf,
    #{storage_type := OldType,
      flow_control := #{deliver_release_interval := OldDeliverval},
      clear_interval := OldClearInterval} = emqx_config:get([?APP]),
    emqx_config:put([?APP], Conf),
    case Enable of
          true ->
              case OldType of
                StorageType ->
                      safe_release_context(release_lockers),
                      {keep_state_and_data,
                       lists:foldl(fun({Old, New, Timer}, Acc) ->
                                           check_timer_change(Old, New, Timer, Acc)
                                   end,
                                   [{reply, From, ok}],
                                   [{OldClearInterval, ClearInterval, clear_expired}
                                   , {OldDeliverval, Deliverval, release_deliver_lockers}])};
                  _ ->
                      try
                          emqx_retainer_storage:handle_close()
                      after
                          gen_statem:reply(From, ok),
                          safe_release_context(abort),
                          connect(reset_data(Data), next_state)
                    end
            end;
        _ ->
            safe_release_context(abort),
            {next_state, disable, Data, {reply, From, ok}}
    end.

-spec check_timer_change(non_neg_integer(),
                         non_neg_integer(),
                         atom(),
                         list(gen_statem:action())) -> ok.
check_timer_change(X, X, _, Actions) -> Actions;
check_timer_change(0, X, Timer, Actions) ->
    [{{timeout, Timer}, X, Timer} | Actions];
check_timer_change(_, 0, Timer, Actions) ->
    [{{timeout, Timer}, cancel} | Actions].
