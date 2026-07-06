-module(pobox_weight_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [usage_detailed_unweighted, weighted_post_and_drain,
          weighted_overflow_drops_to_fit,
          weighted_overflow_keep_old, weighted_overflow_stack,
          weighted_oversized_rejected].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

-define(wait_msg(PAT, RET),
    (fun() -> receive PAT -> RET after 2000 -> error({wait_too_long}) end end)()).

%%%%%%%%%%%%%
%%% TESTS %%%
%%%%%%%%%%%%%

%% A1: usage_detailed/1 works on an UNWEIGHTED box, where each message implicitly
%% weighs 1 (so weight == count) and there is no weight cap (max_weight == infinity).
usage_detailed_unweighted(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 5,
                                   type => queue, initial_state => passive}),
    [pobox:post(Box, N) || N <- lists:seq(1, 3)],
    #{count := 3, max := 5, weight := 3, max_weight := infinity} =
        pobox:usage_detailed(Box),
    unlink(Box),
    exit(Box, shutdown).

%% A2: a weighted box (max_weight set) records a per-message weight supplied via
%% post/3; usage_detailed reports the running total and the cap; draining returns
%% the messages UNWRAPPED and resets the weight back to 0.
weighted_post_and_drain(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => queue, initial_state => passive}),
    pobox:post(Box, a, 30),
    pobox:post(Box, b, 20),
    #{count := 2, max := 10, weight := 50, max_weight := 100} =
        pobox:usage_detailed(Box),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    [a, b] = ?wait_msg({mail, Box, Msgs, 2, 0}, Msgs),
    #{count := 0, weight := 0, max_weight := 100} = pobox:usage_detailed(Box),
    unlink(Box),
    exit(Box, shutdown).

%% A3: on a weighted queue box, an insert that pushes total weight over the cap
%% drops from the drop-end (oldest first) until the weight fits again — even though
%% the count cap is nowhere near hit. Dropped weight leaves the running total.
weighted_overflow_drops_to_fit(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => queue, initial_state => passive}),
    pobox:post(Box, a, 50),
    pobox:post(Box, b, 40),
    pobox:post(Box, c, 30),               %% 50+40+30 = 120 > 100 -> drop a (oldest)
    #{count := 2, weight := 70, max_weight := 100} = pobox:usage_detailed(Box),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    {[b, c], 2, 1} = ?wait_msg({mail, Box, M, Cnt, Lost}, {M, Cnt, Lost}),
    unlink(Box),
    exit(Box, shutdown).

%% A4a: a weighted keep_old box rejects the NEW message when it would exceed the
%% weight cap, keeping the older messages.
weighted_overflow_keep_old(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => keep_old, initial_state => passive}),
    pobox:post(Box, a, 50),
    pobox:post(Box, b, 40),
    pobox:post(Box, c, 30),               %% would be 120 > 100 -> reject c (keep old)
    #{count := 2, weight := 90} = maps:with([count, weight], pobox:usage_detailed(Box)),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    {[a, b], 2, 1} = ?wait_msg({mail, Box, M, Cnt, Lost}, {M, Cnt, Lost}),
    unlink(Box),
    exit(Box, shutdown).

%% A4b: a weighted stack box drops the most-recent EXISTING element to fit a new
%% one (keeping the oldest and the newest) — mirroring unweighted stack overflow,
%% NOT dropping the message just posted.
weighted_overflow_stack(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => stack, initial_state => passive}),
    pobox:post(Box, a, 50),
    pobox:post(Box, b, 40),
    pobox:post(Box, c, 30),               %% 120 > 100 -> drop b (old newest), keep a + c
    #{count := 2, weight := 80} = maps:with([count, weight], pobox:usage_detailed(Box)),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    {[c, a], 2, 1} = ?wait_msg({mail, Box, M, Cnt, Lost}, {M, Cnt, Lost}),
    unlink(Box),
    exit(Box, shutdown).

%% A5: a single message heavier than the whole cap can never fit, so it is rejected
%% outright — counted as a drop, but the already-buffered messages are left intact
%% (the buffer is NOT emptied chasing impossible room).
weighted_oversized_rejected(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => queue, initial_state => passive}),
    pobox:post(Box, a, 50),
    pobox:post(Box, big, 200),            %% 200 > 100 -> rejected, a untouched
    #{count := 1, weight := 50} = maps:with([count, weight], pobox:usage_detailed(Box)),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    {[a], 1, 1} = ?wait_msg({mail, Box, M, Cnt, Lost}, {M, Cnt, Lost}),
    unlink(Box),
    exit(Box, shutdown).
