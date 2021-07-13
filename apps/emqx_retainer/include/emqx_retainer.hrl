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

-include_lib("emqx/include/emqx.hrl").

-define(APP, emqx_retainer).
-define(TAB, ?APP).
-define(RETAINER_SHARD, emqx_retainer_shard).
-type topic() :: binary().
-type payload() :: binary().
-type message() :: #message{}.

-define(CONTEXT_TAB, emqx_retainer_ctx).
-record(context, {key :: atom(), value :: term()}).

-type locker() :: context.

-define(DELIVER_SEMAPHORE, deliver_remained_quota).
