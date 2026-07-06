-module(pobox_call_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [call_reply_happy_path].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

-define(wait_msg(PAT, RET),
    (fun() -> receive PAT -> RET after 2000 -> error({wait_too_long}) end end)()).

%%%%%%%%%%%%%
%%% TESTS %%%
%%%%%%%%%%%%%

%% B1: a client calls the PO Box; the owner drains the (wrapped) call, computes a
%% result, and replies DIRECTLY to the client — which receives {ok, Reply}.
call_reply_happy_path(_Config) ->
    {ok, Box} = pobox:start_link(self(), 10, keep_old, notify),
    Owner = self(),
    Client = spawn(fun() ->
        Owner ! {client_result, pobox:call(Box, {add, 2, 3}, 2000)}
    end),
    %% Owner is notified of new data, activates, and drains the call.
    ?wait_msg({mail, Box, new_data}, ok),
    pobox:active(Box, fun(M, S) -> {{ok, M}, S} end, no_state),
    Call = ?wait_msg({mail, Box, [C], 1, 0}, C),
    true = pobox:is_call(Call),
    {'$pobox_call', ReplyTo, {add, A, B}} = Call,
    ok = pobox:reply(ReplyTo, A + B),
    %% Client got the owner's reply directly.
    {ok, 5} = ?wait_msg({client_result, R}, R),
    _ = Client,
    unlink(Box),
    exit(Box, shutdown).
