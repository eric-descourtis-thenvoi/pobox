-module(pobox_preflight_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [mod_buffer_with_opts].

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
