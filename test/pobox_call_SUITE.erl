-module(pobox_call_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [call_reply_happy_path, call_dropped_on_keep_old_full,
          call_dropped_by_filter, call_noproc_on_box_death,
          call_timeout_when_no_reply, call_noproc_unregistered,
          call_queue_overflow_degrades_to_timeout].

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

%% B4a: if the box dies before replying, the caller (monitoring the box) gets noproc.
call_noproc_on_box_death(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, queue, passive),
    Owner = self(),
    _Client = spawn(fun() -> Owner ! {client_result, pobox:call(Box, {req}, 5000)} end),
    timer:sleep(50),
    unlink(Box),
    exit(Box, kill),
    {error, noproc} = ?wait_msg({client_result, R}, R).

%% B4b: no owner reply within the timeout -> {error, timeout}.
call_timeout_when_no_reply(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, queue, passive),
    {error, timeout} = pobox:call(Box, {req}, 100),
    unlink(Box),
    exit(Box, shutdown).

%% B4c: calling an unregistered name -> {error, noproc}, no crash.
call_noproc_unregistered(_Config) ->
    {error, noproc} = pobox:call(no_such_pobox_name, {req}, 100).

%% B4d: a call buffered in a plain queue can be bumped by a later post; that bulk
%% overflow drop is NOT notified (cost-aligned), so the call degrades to timeout.
%% keep_old is the type to use when calls must be drop-notified.
call_queue_overflow_degrades_to_timeout(_Config) ->
    {ok, Box} = pobox:start_link(self(), 1, queue, passive),
    Owner = self(),
    _Client = spawn(fun() -> Owner ! {client_result, pobox:call(Box, {req}, 200)} end),
    timer:sleep(50),
    pobox:post(Box, bump),   %% queue full -> drops the buffered call, not notified
    {error, timeout} = ?wait_msg({client_result, R}, R),
    unlink(Box),
    exit(Box, shutdown).
