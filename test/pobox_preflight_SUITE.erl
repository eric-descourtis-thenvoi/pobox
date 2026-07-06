-module(pobox_preflight_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [mod_buffer_with_opts,
          preflight_valid, preflight_bad_max, preflight_bad_type,
          preflight_module_not_loaded, preflight_missing_callback,
          start_link_fails_fast_on_bad_module,
          preflight_bad_owner_and_heir,
          positional_start_link_fails_fast_on_bad_module,
          opts_only_buffer_survives_overflow,
          preflight_bad_name].

-define(wait_mail(PAT, RET),
    (fun() -> receive PAT -> RET after 2000 -> error({wait_too_long}) end end)()).

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

-define(wait_msg(PAT, RET),
    (fun() -> receive PAT -> RET after 2000 -> error({wait_too_long}) end end)()).

%%%%%%%%%%%%%
%%% TESTS %%%
%%%%%%%%%%%%%

%% C1: a {mod, Mod, Opts} buffer type constructs the custom buffer with Mod:new(Opts).
%% The configurable sample tags every message with the prefix from Opts, so seeing the
%% prefix on drain proves Opts reached new/1.
mod_buffer_with_opts(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 10,
                                   type => {mod, pobox_configurable_buf, my_prefix},
                                   initial_state => passive}),
    pobox:post(Box, hello),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    [{my_prefix, hello}] = ?wait_msg({mail, Box, M, 1, 0}, M),
    unlink(Box),
    exit(Box, shutdown).

%% D1: preflight/1 validates a config WITHOUT starting a process, returning ok for a
%% good config and a descriptive {error, Reason} for a bad one.
preflight_valid(_Config) ->
    ok = pobox:preflight(#{max => 10, type => queue}),
    ok = pobox:preflight(#{max => 10, type => keep_old, initial_state => passive}),
    ok = pobox:preflight(#{max => 10, type => {mod, pobox_queue_buf}}),
    ok = pobox:preflight(#{max => 10, type => {mod, pobox_configurable_buf, foo}}),
    ok = pobox:preflight([{max, 5}, {type, stack}]).            %% proplist form too

preflight_bad_max(_Config) ->
    {error, {bad_max, 0}} = pobox:preflight(#{max => 0, type => queue}),
    {error, {bad_max, -1}} = pobox:preflight(#{max => -1, type => queue}),
    {error, {bad_max, undefined}} = pobox:preflight(#{type => queue}).  %% missing max

preflight_bad_type(_Config) ->
    {error, {bad_type, wat}} = pobox:preflight(#{max => 10, type => wat}),
    {error, {bad_initial_state, sideways}} =
        pobox:preflight(#{max => 10, type => queue, initial_state => sideways}).

preflight_module_not_loaded(_Config) ->
    {error, {module_not_loaded, no_such_pobox_mod}} =
        pobox:preflight(#{max => 10, type => {mod, no_such_pobox_mod}}).

preflight_missing_callback(_Config) ->
    %% pobox_configurable_buf has new/1 but no new/0 -> {mod, Mod} needs new/0
    {error, {missing_callback, {pobox_configurable_buf, new, 0}}} =
        pobox:preflight(#{max => 10, type => {mod, pobox_configurable_buf}}),
    %% pobox_queue_buf has new/0 but no new/1 -> {mod, Mod, Opts} needs new/1
    {error, {missing_callback, {pobox_queue_buf, new, 1}}} =
        pobox:preflight(#{max => 10, type => {mod, pobox_queue_buf, opts}}).

%% D2: start_link fails fast with the descriptive preflight reason for a bad buffer
%% module, instead of a cryptic init crash. Trap exits so a linked init failure in the
%% pre-fix behaviour can't take the test process down.
start_link_fails_fast_on_bad_module(_Config) ->
    Trap = process_flag(trap_exit, true),
    {error, {module_not_loaded, no_such_pobox_mod}} =
        pobox:start_link(#{owner => self(), max => 10, type => {mod, no_such_pobox_mod}}),
    {error, {missing_callback, {pobox_configurable_buf, new, 0}}} =
        pobox:start_link(#{owner => self(), max => 10, type => {mod, pobox_configurable_buf}}),
    process_flag(trap_exit, Trap),
    ok.

%% E1 (review): preflight must also reject a structurally-invalid owner/heir that
%% start_link (via validate_opts) would refuse — otherwise "preflight then trust" lies.
preflight_bad_owner_and_heir(_Config) ->
    {error, {bad_owner, "nope"}} =
        pobox:preflight(#{max => 10, type => queue, owner => "nope"}),
    {error, {bad_heir, "nope"}} =
        pobox:preflight(#{max => 10, type => queue, heir => "nope"}),
    %% valid owner/heir shapes still pass
    ok = pobox:preflight(#{max => 10, type => queue, owner => self()}),
    ok = pobox:preflight(#{max => 10, type => queue, heir => some_heir_name}),
    ok = pobox:preflight(#{max => 10, type => queue}).           %% heir defaults undefined

%% E2 (review): the positional start_link forms must fail fast on a bad module too,
%% not just the map/proplist forms.
positional_start_link_fails_fast_on_bad_module(_Config) ->
    Trap = process_flag(trap_exit, true),
    {error, {module_not_loaded, no_such_pobox_mod}} =
        pobox:start_link(self(), 10, {mod, no_such_pobox_mod}),
    {error, {module_not_loaded, no_such_pobox_mod}} =
        pobox:start_link(self(), 10, {mod, no_such_pobox_mod, some_opts}, passive),
    process_flag(trap_exit, Trap),
    ok.

%% E-H1 (review): an opts-only {mod,Mod,Opts} buffer (only new/1, no new/0, no
%% push_drop/2) must survive an overflow. The drop-all reset previously called Mod:new/0
%% and crashed; it now empties via the mandatory Mod:drop/2.
opts_only_buffer_survives_overflow(_Config) ->
    {ok, Box} = pobox:start_link(#{owner => self(), max => 1,
                                   type => {mod, pobox_opts_only_buf, ignored},
                                   initial_state => passive}),
    pobox:post(Box, m1),
    pobox:post(Box, m2),                     %% overflow -> drop-all reset path
    true = is_process_alive(Box),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    [m2] = ?wait_mail({mail, Box, Msgs, 1, 1}, Msgs),
    unlink(Box), exit(Box, shutdown).

%% E-M1 (review): preflight must validate `name` too (the class E1 closed for
%% owner/heir, left open for name) — else preflight==ok but start_link raises badarg.
preflight_bad_name(_Config) ->
    {error, {bad_name, "bad"}} =
        pobox:preflight(#{max => 10, type => queue, name => "bad"}),
    {error, {bad_name, {not_a, name}}} =
        pobox:preflight(#{max => 10, type => queue, name => {not_a, name}}),
    %% valid name shapes still pass
    ok = pobox:preflight(#{max => 10, type => queue, name => a_name}),
    ok = pobox:preflight(#{max => 10, type => queue, name => {global, g}}),
    ok = pobox:preflight(#{max => 10, type => queue}).           %% name defaults undefined
