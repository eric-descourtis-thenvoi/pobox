-module(pobox_async_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [async_post_and_await].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

%%%%%%%%%%%%%
%%% TESTS %%%
%%%%%%%%%%%%%

%% F1: post_async/2 fires a post without blocking and returns a request id (a promise);
%% post_await/2 collects its result (ok, or full when the box is full) — the async
%% analogue of post_sync/3, so many posts can be submitted before any is awaited.
async_post_and_await(_Config) ->
    {ok, Box} = pobox:start_link(self(), 2, keep_old, passive),
    R1 = pobox:post_async(Box, a),
    R2 = pobox:post_async(Box, b),
    R3 = pobox:post_async(Box, c),          %% box full (max 2) -> full
    ok   = pobox:post_await(R1, 5000),
    ok   = pobox:post_await(R2, 5000),
    full = pobox:post_await(R3, 5000),
    unlink(Box),
    exit(Box, shutdown).
