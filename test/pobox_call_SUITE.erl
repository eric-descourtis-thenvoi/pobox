-module(pobox_call_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [call_reply_happy_path, call_dropped_on_keep_old_full,
          call_dropped_by_filter].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

-define(wait_msg(PAT, RET),
    (fun() -> receive PAT -> RET after 2000 -> error({wait_too_long}) end end)()).

%%%%%%%%%%%%%
%%% TESTS %%%
%%%%%%%%%%%%%

%% B1: a client calls the PO Box; the owner drains the (wrapped) call, computes a
%% result, and replies DIRECTLY to the client — which receives {ok, Reply}.
call_reply_happy_path(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, keep_old, notify),
    Owner = self(),
    Client = spawn(fun() ->
        Owner ! {client_result, pobox:call(Box, {add, 2, 3}, 2000)}
    end),
    %% Owner is notified of new data, activates, and drains the call.
    ?wait_msg({mail, Box, new_data}, ok),
    pobox:active(Box, fun(M, S) -> {{ok, M}, S} end, no_state),
    Call = ?wait_msg({mail, Box, [C], 1, 0}, C),
    true = pobox:is_call(Call),
    {'$pobox_call', ReplyTo, {add, A, B}} = Call,
    ok = pobox:reply(ReplyTo, A + B),
    %% Client got the owner's reply directly.
    {ok, 5} = ?wait_msg({client_result, R}, R),
    _ = Client,
    unlink(Box),
    exit(Box, shutdown).

%% B2: on a full keep_old box, a new call is rejected at admission; the caller is
%% told {error, dropped} promptly (not left to time out). keep_old is the drop-safe
%% substrate for calls.
call_dropped_on_keep_old_full(_Config) ->
    {ok, Box} = pobox:start_link(self(), 1, keep_old, passive),
    pobox:post(Box, filler),                       %% box is now full (size 1 of 1)
    {error, dropped} = pobox:call(Box, {req}, 2000),
    unlink(Box),
    exit(Box, shutdown).

%% B3: if the owner's active filter drops a call during a drain, the caller is told
%% {error, dropped} rather than being left to time out.
call_dropped_by_filter(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, queue, notify),
    Owner = self(),
    _Client = spawn(fun() ->
        Owner ! {client_result, pobox:call(Box, {req}, 2000)}
    end),
    ?wait_msg({mail, Box, new_data}, ok),
    pobox:active(Box, fun(_M, S) -> {drop, S} end, no_state),   %% owner drops the call
    ?wait_msg({mail, Box, [], 0, 1}, ok),
    {error, dropped} = ?wait_msg({client_result, R}, R),
    unlink(Box),
    exit(Box, shutdown).
