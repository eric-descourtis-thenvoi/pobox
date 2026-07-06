%%%-------------------------------------------------------------------
%% @doc Sample custom buffer that supports weighting: a plain FIFO queue that
%% also implements the optional `drop_one/1' callback, so it can be used as the
%% `{mod, pobox_weighted_buf}' type on a box started with `max_weight'.
%% @end
%%%-------------------------------------------------------------------
-module(pobox_weighted_buf).

-behaviour(pobox_buf).
%% API
-export([new/0, push/2, pop/1, drop/2, push_drop/2, drop_one/1]).

new() ->
  queue:new().

push(Msg, Q) ->
  queue:in(Msg, Q).

pop(Q) ->
  queue:out(Q).

drop(N, Q) ->
  element(2, queue:split(N, Q)).

push_drop(Msg, Q) ->
  push(Msg, drop(1, Q)).

%% Remove one element from the drop-end (front / oldest) and return it, so pobox
%% can account its weight and notify it if it is a call.
drop_one(Q) ->
  queue:out(Q).
