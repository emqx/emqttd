%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module implements a request handler based on emqx_client.
%% A request handler is a MQTT client which subscribes to a request topic,
%% processes the requests then send response to another topic which is
%% subscribed by the request sender.
%% This code is in test directory because request and response are pure
%% client-side behaviours.

-module(emqx_request_handler).

-export([start_link/4, stop/1]).

-type qos() :: emqx_mqtt_types:qos_name() | emqx_mqtt_types:qos().
-type topic() :: emqx_topic:topic().
-type handler() :: fun((CorrData :: binary(), ReqPayload :: binary()) -> RspPayload :: binary()).

-spec start_link(topic(), qos(), handler(), emqx_client:options()) ->
        {ok, pid()} | {error, any()}.
start_link(RequestTopic, QoS, RequestHandler, Options0) ->
    Parent = self(),
    MsgHandler = make_msg_handler(RequestHandler, Parent),
    Options = [{msg_handler, MsgHandler} | Options0],
    case emqx_client:start_link(Options) of
        {ok, Pid} ->
            {ok, _} = emqx_client:connect(Pid),
            try subscribe(Pid, RequestTopic, QoS) of
                ok -> {ok, Pid};
                {error, _} = Error -> Error
            catch
                C : E : S ->
                    emqx_client:stop(Pid),
                    {error, {C, E, S}}
            end;
        {error, _} = Error -> Error
    end.

stop(Pid) ->
    emqx_client:disconnect(Pid).

make_msg_handler(RequestHandler, Parent) ->
    #{publish => fun(Msg) -> handle_msg(Msg, RequestHandler, Parent) end,
      puback => fun(_Ack) -> ok end,
      disconnected => fun(_Reason) -> ok end
     }.

handle_msg(ReqMsg, RequestHandler, Parent) ->
    #{qos := QoS, properties := Props, payload := ReqPayload} = ReqMsg,
    case maps:find('Response-Topic', Props) of
        {ok, RspTopic} when RspTopic =/= <<>> ->
            CorrData = maps:get('Correlation-Data', Props),
            RspProps = maps:without(['Response-Topic'], Props),
            RspPayload = RequestHandler(CorrData, ReqPayload),
            emqx_logger:debug("~p sending response msg to topic ~s with~n"
                              "corr-data=~p~npayload=~p",
                              [?MODULE, RspTopic, CorrData, RspPayload]),
            ok = send_response(RspTopic, RspProps, RspPayload, QoS);
        _ ->
            Parent ! {discarded, ReqPayload},
            ok
    end.

send_response(Topic, Properties, Payload, QoS) ->
    %% This function is evaluated by emqx_client itself.
    %% hence delegate to another temp process for the loopback gen_statem call.
    Client = self(),
    _ = spawn_link(fun() ->
                           case emqx_client:publish(Client, Topic, Properties, Payload, [{qos, QoS}]) of
                               ok -> ok;
                               {ok, _} -> ok;
                               {error, Reason} -> exit({failed_to_publish_response, Reason})
                           end
                   end),
    ok.

subscribe(Client, Topic, QoS) ->
    {ok, _Props, _QoS} =
        emqx_client:subscribe(Client, [{Topic, [{rh, 2}, {rap, false},
                                       {nl, true}, {qos, QoS}]}]),
    ok.



