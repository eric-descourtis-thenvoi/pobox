%%%-------------------------------------------------------------------
%% @copyright Eric des Courtis
%% @author Eric des Courtis <eric.descourtis@mitel.com>
%% @doc Generic message buffer behaviour. For more
%% information, see README.txt
%% @end
%%%-------------------------------------------------------------------
-module(pobox_buf).

%% Behaviour API
%% A buffer is constructed by new/0 (for the `{mod, Mod}' type) or new/1 (for the
%% `{mod, Mod, Opts}' type). Implement whichever arity matches how it is instantiated;
%% both are optional so an opts-only buffer need not define new/0.
-callback new() -> Buf :: any().
-callback new(Opts :: any()) -> Buf :: any().
-callback push(Msg :: any(), Buf :: any()) -> Buf :: any().
-callback pop(Buf :: any()) -> {empty, Buf :: any()} | {{value, Msg :: any()}, Buf :: any()}.
-callback drop(N :: pos_integer(), Buf :: any()) -> Buf ::any().
-callback push_drop(Msg :: any(), Buf :: any()) -> Buf :: any().
-optional_callbacks([new/0, new/1, push_drop/2]).


