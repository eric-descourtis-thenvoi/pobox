-module(pobox_weight_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [usage_detailed_unweighted, weighted_post_and_drain,
          weighted_overflow_drops_to_fit,
          weighted_overflow_keep_old, weighted_overflow_stack,
          weighted_oversized_rejected, weighted_post_sync_full,
          weighted_detailed_mail, weighted_resize, weighted_mod_buffer,
          weighted_post_sync_weightless_full, resize_rejects_weighting_flip].

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

%% A6: post_sync/4 on a weighted box replies `full` when the message would not fit
%% under the weight cap (or is oversized), and `ok` otherwise. Uses keep_old so a
%% `full` reply means the message really was not stored.
weighted_post_sync_full(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => keep_old, initial_state => passive}),
    ok   = pobox:post_sync(Box, a, 60, 5000),
    ok   = pobox:post_sync(Box, b, 40, 5000),   %% 60+40 = 100, exactly at the cap
    full = pobox:post_sync(Box, c, 10, 5000),   %% 110 > 100 -> does not fit
    full = pobox:post_sync(Box, big, 200, 5000),%% oversized -> full
    #{count := 2, weight := 100} = maps:with([count, weight], pobox:usage_detailed(Box)),
    unlink(Box),
    exit(Box, shutdown).

%% A7: with detailed_mail => true the drained mail carries a metrics map instead of
%% the count/lost scalars, reporting delivered weight and the weight lost since the
%% last drain (here: a, dropped at insert time, weighs 50).
weighted_detailed_mail(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => queue, initial_state => passive,
                                   detailed_mail => true}),
    pobox:post(Box, a, 50),
    pobox:post(Box, b, 40),
    pobox:post(Box, c, 30),               %% drop a (50); buffered b+c weigh 70
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    {[b, c], #{count := 2, lost := 1, weight := 70, lost_weight := 50}} =
        ?wait_msg({mail, Box, M, Meta}, {M, Meta}),
    unlink(Box),
    exit(Box, shutdown).

%% A8: resize accepts a map to retune the weight cap (and/or count cap) at runtime;
%% shrinking either cap drops from the drop-end to fit. Integer resize on a weighted
%% box stays weight-consistent.
weighted_resize(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => queue, initial_state => passive}),
    pobox:post(Box, a, 40),
    pobox:post(Box, b, 40),                       %% weight 80, count 2
    ok = pobox:resize(Box, #{max_weight => 50}),  %% shrink weight cap -> drop a (oldest)
    #{count := 1, weight := 40, max_weight := 50} = pobox:usage_detailed(Box),
    ok = pobox:resize(Box, #{max_weight => 100}), %% grow weight cap back
    pobox:post(Box, c, 40),                       %% [b, c], weight 80
    ok = pobox:resize(Box, 1),                    %% count cap -> drop b, weight-consistent
    #{count := 1, weight := 40} = maps:with([count, weight], pobox:usage_detailed(Box)),
    unlink(Box),
    exit(Box, shutdown).

%% A9: a weighted box on a custom {mod,_} buffer uses the buffer's drop_one/1 to
%% account dropped weight — same drop-to-fit behaviour as the built-in queue.
weighted_mod_buffer(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => {mod, pobox_weighted_buf},
                                   initial_state => passive}),
    pobox:post(Box, a, 50),
    pobox:post(Box, b, 40),
    pobox:post(Box, c, 30),               %% 120 > 100 -> drop a (oldest via drop_one/1)
    #{count := 2, weight := 70} = maps:with([count, weight], pobox:usage_detailed(Box)),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    {[b, c], 2, 1} = ?wait_msg({mail, Box, M, Cnt, Lost}, {M, Cnt, Lost}),
    unlink(Box),
    exit(Box, shutdown).

%% E1 (review HIGH): the weightless post_sync/2,3 on a weighted box must consult the
%% weight cap for its full/ok reply — a weight-1 message can still be rejected because
%% the WEIGHT cap is saturated even though the count cap is nowhere near full.
weighted_post_sync_weightless_full(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 5,
                                   type => keep_old, initial_state => passive}),
    ok   = pobox:post_sync(Box, a, 5, 5000),   %% weight 5, saturates the weight cap
    full = pobox:post_sync(Box, b),            %% weight-1 can't fit (5+1 > 5) -> full
    #{count := 1, weight := 5} = maps:with([count, weight], pobox:usage_detailed(Box)),
    unlink(Box),
    exit(Box, shutdown).

%% E2 (review HIGH): resize must not flip a box between weighted and unweighted (which
%% would leave wrapped/unwrapped elements mixed in the buffer). Such a resize is
%% rejected with {error, badarg} and leaves the box healthy.
resize_rejects_weighting_flip(_Config) ->
    {ok, B1} = pobox:start_link(#{owner => self(), max => 10,
                                  type => queue, initial_state => passive}),
    {error, badarg} = pobox:resize(B1, #{max_weight => 5}),   %% unweighted -> weighted
    pobox:post(B1, x),
    #{count := 1, max_weight := infinity} =
        maps:with([count, max_weight], pobox:usage_detailed(B1)),
    unlink(B1), exit(B1, shutdown),
    {ok, B2} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                  type => queue, initial_state => passive}),
    {error, badarg} = pobox:resize(B2, #{max_weight => infinity}), %% weighted -> unweighted
    pobox:post(B2, y, 50),
    #{count := 1, weight := 50, max_weight := 100} =
        maps:with([count, weight, max_weight], pobox:usage_detailed(B2)),
    unlink(B2), exit(B2, shutdown).
