-module(pobox_weight_validation_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [bad_max_weight_rejected, bad_detailed_mail_rejected,
          resize_bad_max_weight_badarg, resize_bad_max_badarg,
          weighted_mod_without_drop_one_fails_fast,
          good_configs_still_start,
          anonymous_weighted_post].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

starts_badarg(Opts) ->
    case catch pobox:start_link(Opts#{owner => self(), initial_state => passive}) of
        {'EXIT', {badarg, _}} -> ok;
        {ok, Pid} -> unlink(Pid), exit(Pid, shutdown), accepted;
        Other -> Other
    end.

%% E-H1: malformed max_weight must be refused at start (badarg), not silently accepted
%% (a 0/negative cap black-holes every post; a non-integer disables the cap).
bad_max_weight_rejected(_Config) ->
    [ok = starts_badarg(#{max => 10, type => queue, max_weight => MW})
     || MW <- [0, -5, foo, 1.5]].

%% E-H1: a non-boolean detailed_mail must be refused at start — otherwise the box
%% starts clean and crashes (with its linked owner) on the first drain.
bad_detailed_mail_rejected(_Config) ->
    ok = starts_badarg(#{max => 10, type => queue, max_weight => 100,
                         detailed_mail => notabool}).

%% E-H2: resize with a malformed max_weight must return {error, badarg} (the contract),
%% not crash the box.
resize_bad_max_weight_badarg(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => queue, initial_state => passive}),
    [ {error, badarg} = pobox:resize(Box, #{max_weight => MW}) || MW <- [0, -1, 3.5, foo] ],
    true = is_process_alive(Box),
    unlink(Box), exit(Box, shutdown).

%% E-H3: resize with a malformed max must return {error, badarg}, not silently corrupt
%% the count cap.
resize_bad_max_badarg(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => queue, initial_state => passive}),
    [ {error, badarg} = pobox:resize(Box, #{max => M}) || M <- [0, -5, foo] ],
    true = is_process_alive(Box),
    unlink(Box), exit(Box, shutdown).

%% E-H4: a weighted {mod,_} buffer without drop_one/1 must fail fast at start (before it
%% can crash on the first overflow and take the owner down).
weighted_mod_without_drop_one_fails_fast(_Config) ->
    %% pobox_queue_buf implements the core callbacks but NOT drop_one/1.
    R = catch pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                 type => {mod, pobox_queue_buf}, initial_state => passive}),
    case R of
        {error, {missing_callback, {pobox_queue_buf, drop_one, 1}}} -> ok;
        {'EXIT', {badarg, _}} -> ok;
        {ok, Pid} -> unlink(Pid), exit(Pid, shutdown), error(started_without_drop_one)
    end.

%% Guard: valid configs (including a weighted {mod,_} WITH drop_one/1) still start.
good_configs_still_start(_Config) ->
    Ok = fun(Opts) ->
        {ok, Pid} = pobox:start_link(Opts#{owner => self(), initial_state => passive}),
        unlink(Pid), exit(Pid, shutdown)
    end,
    Ok(#{max => 10, type => queue}),
    Ok(#{max => 10, type => queue, max_weight => 100}),
    Ok(#{max => 10, type => queue, max_weight => infinity}),
    Ok(#{max => 10, type => keep_old, max_weight => 100, detailed_mail => true}),
    Ok(#{max => 10, type => {mod, pobox_weighted_buf}, max_weight => 100}).

%% E-L2 (review): a raw anonymous weighted post `Box ! {post, Msg, W}` must be honored
%% on a weighted box (symmetry with `Box ! {post, Msg}`), not silently ignored.
anonymous_weighted_post(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10, max_weight => 100,
                                   type => queue, initial_state => passive}),
    Box ! {post, a, 40},
    #{count := 1, weight := 40} = maps:with([count, weight], pobox:usage_detailed(Box)),
    unlink(Box), exit(Box, shutdown).
