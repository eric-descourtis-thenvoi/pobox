-module(pobox_async_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [async_post_and_await, pipelined_posts, async_await_noproc,
          async_await_reawaitable_after_timeout, async_await_infinity,
          async_await1_noproc].

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

%% F2: a whole burst is submitted first (all post_async, none blocking), then collected.
%% All land, in order, each answered ok.
pipelined_posts(_Config) ->
    N = 100,
    {ok, Box} = pobox:start_link(self(), 1000, queue, passive),
    ReqIds = [pobox:post_async(Box, I) || I <- lists:seq(1, N)],   %% fire all, non-blocking
    Results = [pobox:post_await(R, 5000) || R <- ReqIds],           %% then collect all
    [ok] = lists:usort(Results),
    pobox:active(Box, fun(X, S) -> {{ok, X}, S} end, no_state),
    receive
        {mail, Box, Msgs, N, 0} -> Msgs = lists:seq(1, N)
    after 5000 ->
        error(no_mail)
    end,
    unlink(Box),
    exit(Box, shutdown).

%% F3: awaiting a promise whose box is gone returns {error, noproc}.
async_await_noproc(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, queue, passive),
    unlink(Box),
    Ref = monitor(process, Box),
    exit(Box, shutdown),
    receive {'DOWN', Ref, process, Box, _} -> ok after 2000 -> error(box_not_dead) end,
    ReqId = pobox:post_async(Box, x),
    {error, noproc} = pobox:post_await(ReqId, 2000).

%% E1 (review M1/M3): a timed-out promise must stay valid and be re-awaitable — the
%% documented "promise" contract. Suspend the box so the first await deterministically
%% times out, then resume and re-await for the real result. (receive_response abandons
%% on timeout and would lose it; wait_response does not.)
async_await_reawaitable_after_timeout(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, keep_old, passive),
    sys:suspend(Box),
    ReqId = pobox:post_async(Box, msg),
    timeout = pobox:post_await(ReqId, 50),     %% box suspended -> times out
    sys:resume(Box),
    ok = pobox:post_await(ReqId, 5000),        %% re-await gets the result
    unlink(Box), exit(Box, shutdown).

%% E2 (review M3): post_await/1 (infinity) happy path — previously uncovered.
async_await_infinity(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, keep_old, passive),
    ReqId = pobox:post_async(Box, msg),
    ok = pobox:post_await(ReqId),
    unlink(Box), exit(Box, shutdown).

%% E3 (review M2): post_await/1 can return {error, noproc} (a box dying during an
%% infinity await), which the spec must admit.
async_await1_noproc(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, queue, passive),
    unlink(Box),
    Ref = monitor(process, Box),
    exit(Box, shutdown),
    receive {'DOWN', Ref, process, Box, _} -> ok after 2000 -> error(box_not_dead) end,
    ReqId = pobox:post_async(Box, x),
    {error, noproc} = pobox:post_await(ReqId).
