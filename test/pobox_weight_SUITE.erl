-module(pobox_weight_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [usage_detailed_unweighted, weighted_post_and_drain].

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
