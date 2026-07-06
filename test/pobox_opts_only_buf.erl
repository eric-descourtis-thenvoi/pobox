%% Test-support buffer: implements ONLY new/1 + the mandatory push/2, pop/1, drop/2.
%% It deliberately omits new/0 AND the optional push_drop/2, so an overflow takes the
%% push_drop fallback and hits the drop-all reset — the H1 crash path (Mod:new/0).
-module(pobox_opts_only_buf).
-behaviour(pobox_buf).
-export([new/1, push/2, pop/1, drop/2]).

new(_Opts) -> queue:new().
push(Msg, Q) -> queue:in(Msg, Q).
pop(Q) -> queue:out(Q).
drop(N, Q) -> element(2, queue:split(N, Q)).
