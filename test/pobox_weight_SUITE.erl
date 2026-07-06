-module(pobox_weight_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [usage_detailed_unweighted].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

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
