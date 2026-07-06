-module(pobox_call_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [call_reply_happy_path, call_dropped_on_keep_old_full,
          call_dropped_by_filter, call_noproc_on_box_death,
          call_timeout_when_no_reply, call_noproc_unregistered,
          call_queue_overflow_degrades_to_timeout,
          concurrent_calls_each_get_their_own_reply,
          call_timeout_leaves_no_stray_message,
          call_via_local_name,
          call_rejects_unknown_opts, call_misc_coverage].

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

%% B5: many clients call concurrently; the owner drains cohorts and replies to each,
%% and every client receives ITS OWN answer (replies are routed per-caller).
concurrent_calls_each_get_their_own_reply(_Config) ->
    N = 20,
    {ok, Box} = pobox:start_link(self(), 100, keep_old, passive),
    Owner = self(),
    _ = [spawn(fun() -> Owner ! {res, I, pobox:call(Box, {sq, I}, 5000)} end)
         || I <- lists:seq(1, N)],
    ok = serve_calls(Box, N),
    Results = collect_results(N, #{}),
    lists:foreach(fun(I) ->
        #{I := {ok, Sq}} = Results,
        Sq = I * I
    end, lists:seq(1, N)),
    unlink(Box),
    exit(Box, shutdown).

%% Robustness stress for the reply-vs-timeout race that motivated the E1 flush fix:
%% many rounds where a caller uses a 1 ms timeout while the owner replies at roughly
%% the same moment. Each caller must end up with exactly one clean outcome — {ok,_},
%% {error,timeout} or {error,dropped} — and NO orphaned pobox-internal message in its
%% mailbox. (The exact sub-instruction race the fix guards is not deterministically
%% reproducible; this exercises the path and guards against gross regressions.)
call_timeout_leaves_no_stray_message(_Config) ->
    {ok, Box} = pobox:start_link(self(), 100, keep_old, passive),
    Owner = self(),
    lists:foreach(fun(_) -> race_round(Owner, Box) end, lists:seq(1, 500)),
    unlink(Box),
    exit(Box, shutdown).

race_round(Owner, Box) ->
    _Caller = spawn(fun() ->
        R = pobox:call(Box, {req}, 1),   %% 1 ms timeout — races the reply below
        Stray = [M || M <- element(2, process_info(self(), messages)),
                      is_pobox_internal(M)],
        Owner ! {round_done, R, Stray}
    end),
    %% Owner drains the call as soon as it lands and replies — racing the timeout.
    _ = drain_and_reply_once(Box),
    receive
        {round_done, R, Stray} ->
            [] = Stray,
            true = lists:member(R, [{ok, answered}, {error, timeout}, {error, dropped}])
    after 5000 ->
        error(round_timeout)
    end.

drain_and_reply_once(Box) ->
    pobox:active(Box, fun(M, S) -> {{ok, M}, S} end, no_state),
    receive
        {mail, Box, [Call], 1, 0} ->
            {'$pobox_call', ReplyTo, {req}} = Call,
            pobox:reply(ReplyTo, answered);
        {mail, Box, [], 0, _} ->
            ok
    after 5000 ->
        error(no_call_to_drain)
    end.

is_pobox_internal({'$pobox_reply', _, _}) -> true;
is_pobox_internal({'$pobox_drop', _}) -> true;
is_pobox_internal(_) -> false.

%%%%%%%%%%%%%%%
%%% HELPERS %%%
%%%%%%%%%%%%%%%

%% Repeatedly drain the box and answer each call, until Remaining calls are served.
serve_calls(_Box, 0) -> ok;
serve_calls(Box, Remaining) ->
    pobox:active(Box, fun(M, S) -> {{ok, M}, S} end, no_state),
    receive
        {mail, Box, Calls, _Count, _Lost} ->
            [begin
                 {'$pobox_call', ReplyTo, {sq, I}} = C,
                 pobox:reply(ReplyTo, I * I)
             end || C <- Calls],
            serve_calls(Box, Remaining - length(Calls))
    after 5000 ->
        error({unserved, Remaining})
    end.

collect_results(0, Acc) -> Acc;
collect_results(N, Acc) ->
    receive
        {res, I, R} -> collect_results(N - 1, Acc#{I => R})
    after 5000 ->
        error({missing_results, N})
    end.

%% E1 (review M1): call/2,3 must accept a {local, Name} box reference (a valid name())
%% instead of crashing with function_clause in where/1.
call_via_local_name(_Config) ->
    {ok, Box} = pobox:start_link({local, pobox_call_local}, self(), 10, keep_old, notify),
    Owner = self(),
    _ = spawn(fun() ->
        Owner ! {client_result, pobox:call({local, pobox_call_local}, ping, 2000)}
    end),
    ?wait_msg({mail, Box, new_data}, ok),
    pobox:active(Box, fun(M, S) -> {{ok, M}, S} end, no_state),
    {'$pobox_call', ReplyTo, ping} = ?wait_msg({mail, Box, [C], 1, 0}, C),
    ok = pobox:reply(ReplyTo, pong),
    {ok, pong} = ?wait_msg({client_result, R}, R),
    unlink(Box), exit(Box, shutdown).

%% E-L1 (review): call/3's options map must reject unknown keys (e.g. a `timout` typo)
%% instead of silently swallowing them and using defaults.
call_rejects_unknown_opts(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, keep_old, notify),
    {'EXIT', {badarg, _}} = (catch pobox:call(Box, req, #{timout => 100})),
    {'EXIT', {badarg, _}} = (catch pobox:call(Box, req, #{weight => 1, bogus => x})),
    unlink(Box), exit(Box, shutdown).

%% E-L3 (review): coverage for call/2 default, reply/2 guard, is_call/1 negatives, and
%% a call over a {mod,_} buffer.
call_misc_coverage(_Config) ->
    false = pobox:is_call(plain_message),
    false = pobox:is_call({'$pobox_call', not_a_ref, req}),   %% tag slot not a reference
    true  = pobox:is_call({'$pobox_call', make_ref(), req}),
    {'EXIT', {function_clause, _}} = (catch pobox:reply(not_a_ref, hi)),
    %% call/2 (default timeout) over a {mod, _} buffer
    {ok, Box} = pobox:start_link(self(), 10, {mod, pobox_queue_buf}, notify),
    Owner = self(),
    _ = spawn(fun() -> Owner ! {client_result, pobox:call(Box, {sq, 4})} end),
    ?wait_msg({mail, Box, new_data}, ok),
    pobox:active(Box, fun(M, S) -> {{ok, M}, S} end, no_state),
    {'$pobox_call', ReplyTo, {sq, N}} = ?wait_msg({mail, Box, [C], 1, 0}, C),
    ok = pobox:reply(ReplyTo, N * N),
    {ok, 16} = ?wait_msg({client_result, R}, R),
    unlink(Box), exit(Box, shutdown).
