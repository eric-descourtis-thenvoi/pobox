%%%-------------------------------------------------------------------
%% @doc Sample custom buffer that takes construction-time configuration via the
%% optional `new/1' callback. It is a FIFO queue that tags every stored message
%% with a prefix supplied through the `{mod, pobox_configurable_buf, Prefix}'
%% buffer type, demonstrating that `Opts' reaches `new/1'.
%% @end
%%%-------------------------------------------------------------------
-module(pobox_configurable_buf).

-behaviour(pobox_buf).
-export([new/1, push/2, pop/1, drop/2, push_drop/2]).

new(Prefix) ->
  {Prefix, queue:new()}.

push(Msg, {Prefix, Q}) ->
  {Prefix, queue:in({Prefix, Msg}, Q)}.

pop({Prefix, Q}) ->
  case queue:out(Q) of
    {{value, Msg}, Q2} -> {{value, Msg}, {Prefix, Q2}};
    {empty, Q2}        -> {empty, {Prefix, Q2}}
  end.

drop(N, {Prefix, Q}) ->
  {Prefix, element(2, queue:split(N, Q))}.

push_drop(Msg, {Prefix, Q}) ->
  push(Msg, {Prefix, element(2, queue:split(1, Q))}).
