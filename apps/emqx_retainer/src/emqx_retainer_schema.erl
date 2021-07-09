-module(emqx_retainer_schema).

-include_lib("typerefl/include/types.hrl").

-type storage_type() :: builtin
                      | mysql.  %% not support, but in plan

-type builtin_storage_type() :: ram | disc | disc_only.
-type redo_failure_strategy() :: log_only | throw.

-reflect_type([ storage_type/0, builtin_storage_type/0
              , redo_failure_strategy/0]).

-export([structs/0, fields/1]).

-define(TYPE(Type), hoconsc:t(Type)).

structs() -> ["emqx_retainer"].

fields("emqx_retainer") ->
    [ {enable, t(boolean(), false)}
    , {storage_type, t(storage_type(), builtin)}
    , {valid_duration, t(emqx_schema:duration_ms(), "0s")}
    , {clear_interval, t(emqx_schema:duration_ms(), "0s")}
    , {connector, connector()}
    , {action_failure_strategy, action_failure_strategy()}
    , {flow_control, ?TYPE(hoconsc:ref(?MODULE, flow_control))}
    , {size_control, ?TYPE(hoconsc:ref(?MODULE, size_control))}
    ];

fields(builtin_connector) ->
    [{storage_type, t(builtin_storage_type(), ram)}];

fields(flow_control) ->
    [ {read_page_size, t(integer(), 0, fun is_pos_integer/1)}
    , {deliver_quota, t(integer(), 0, fun is_pos_integer/1)}
    , {deliver_release_interval, t(emqx_schema:duration_ms(), "0ms")}
    ];

fields(size_control) ->
    [ {max_retained_messages, t(integer(), 0, fun is_pos_integer/1)}
    , {max_payload_size, t(emqx_schema:bytesize(), "1MB")}
    ];

fields(redo_strategy) ->
    [ {max_try_times, t(integer(), 0, fun is_pos_integer/1)}
    , {try_interval, t(emqx_schema:duration_ms(), "10s")}
    , {failure_strategy, t(redo_failure_strategy(), log_only)}
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
t(Type, Default) ->
    hoconsc:t(Type, #{default => Default}).

t(Type, Default, Validator) ->
    hoconsc:t(Type, #{default => Default,
                      validator => Validator}).

union_array(Item) when is_list(Item) ->
    hoconsc:array(hoconsc:union(Item)).

is_pos_integer(V) ->
    V >= 0.

connector() ->
    #{type => union_array([hoconsc:ref(?MODULE, builtin_connector)])}.

action_failure_strategy() ->
    #{type => union_array([log_only, throw, hoconsc:ref(?MODULE, redo_strategy)])}.











