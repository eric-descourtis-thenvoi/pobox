%%%-------------------------------------------------------------------
%% @copyright Eric des Courtis
%% @author Eric des Courtis <eric.descourtis@mitel.com>
%% @doc Generic message buffer behaviour. For more
%% information, see README.txt
%% @end
%%%-------------------------------------------------------------------
-module(pobox_buf).

%% Behaviour API
-callback new() -> Buf :: any().
-callback push(Msg :: any(), Buf :: any()) -> Buf :: any().
-callback pop(Buf :: any()) -> {empty, Buf :: any()} | {{value, Msg :: any()}, Buf :: any()}.
-callback drop(N :: pos_integer(), Buf :: any()) -> Buf ::any().
-callback push_drop(Msg :: any(), Buf :: any()) -> Buf :: any().
%% drop_one/1 removes one element from the buffer's drop-end and returns it, so pobox
%% can account its weight (weighted boxes) and notify it if it is a call. Required only
%% for a custom buffer used with weighting; optional otherwise.
-callback drop_one(Buf :: any()) -> {empty, Buf :: any()} | {{value, Msg :: any()}, Buf :: any()}.
-optional_callbacks([push_drop/2, drop_one/1]).


