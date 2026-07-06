%%%-------------------------------------------------------------------
%% @copyright Fred Hebert, Geoff Cant
%% @author Fred Hebert <mononcqc@ferd.ca>
%% @author Geoff Cant <nem@erlang.geek.nz>
%% @author Eric des Courtis <eric.descourtis@mitel.com>
%% @doc Generic process that acts as an external mailbox and a
%% message buffer that will drop requests as required. For more
%% information, see README.txt
%% @end
%%%-------------------------------------------------------------------
-module(pobox).
-behaviour(gen_statem).
-compile({no_auto_import,[size/1]}).

-ifdef(namespaced_types).
-record(buf, {type = undefined :: undefined | stack | queue | keep_old | {mod, module()},
              max = undefined :: undefined | max(),
              max_weight = infinity :: infinity | pos_integer(),
              size = 0 :: non_neg_integer(),
              weight = 0 :: non_neg_integer(),
              drop = 0 :: drop(),
              drop_weight = 0 :: non_neg_integer(),
              data = undefined :: undefined | queue:queue() | list()}).
-else.
-record(buf, {type = undefined :: undefined | stack | queue | keep_old | {mod, module()},
              max = undefined :: undefined | max(),
              max_weight = infinity :: infinity | pos_integer(),
              size = 0 :: non_neg_integer(),
              weight = 0 :: non_neg_integer(),
              drop = 0 :: drop(),
              drop_weight = 0 :: non_neg_integer(),
              data = undefined :: undefined | queue() | list()}).
-endif.

-define(
    PROCESS_NAME_GUARD_VIA_OR_GLOBAL(V),
    ((tuple_size(V) == 2 andalso element(1, V) == global) orelse
     (tuple_size(V) == 3 andalso element(1, V) == via))
).

-define(
    PROCESS_NAME_GUARD_NO_PID(V),
    is_atom(V) orelse ?PROCESS_NAME_GUARD_VIA_OR_GLOBAL(V)
).

-define(
    PROCESS_NAME_GUARD(V),
    is_pid(V) orelse ?PROCESS_NAME_GUARD_NO_PID(V)
).

-define(
    PROCESS_NAME_GUARD_WITH_LOCAL_NO_PID(V),
    is_atom(V) orelse (
    (tuple_size(V) == 2 andalso element(1, V) == local) orelse ?PROCESS_NAME_GUARD_VIA_OR_GLOBAL(V))
).

-define(POBOX_START_STATE_GUARD(V), V =:= notify orelse V =:= passive).
-define(POBOX_BUFFER_TYPE_GUARD(V), V =:= queue orelse V =:= stack orelse V =:= keep_old orelse
    (tuple_size(V) == 2 andalso element(1, V) =:= mod andalso is_atom(element(2, V)))
).

-type max() :: pos_integer().
-type drop() :: non_neg_integer().
-type buffer() :: #buf{}.
-type filter() :: fun( (Msg::term(), State::term()) ->
                        {{ok,NewMsg::term()} | drop , State::term()} | skip).

-type in() :: {'post', Msg::term()}.
-type note() :: {'mail', Self::pid(), new_data}.
-type mail() :: {'mail', Self::pid(), Msgs::list(),
                         Count::non_neg_integer(), Lost::drop()}.
-type name() :: {local, atom()} | {global, term()} | atom() | pid() | {via, module(), term()}.

-export_type([max/0, filter/0, in/0, mail/0, note/0]).

-record(state, {buf :: buffer(),
                owner :: name(),
                filter :: undefined | filter(),
                filter_state :: undefined | term(),
                owner_pid :: pid(),
                owner_monitor_ref :: undefined | reference(),
                heir :: undefined | pid() | atom(),
                heir_data :: undefined | term(),
                detailed_mail = false :: boolean(),
                name :: name()}).

-record(pobox_opts, {name :: undefined | name(),
                     owner = self() :: name(),
                     max :: undefined | max(),
                     max_weight = infinity :: infinity | pos_integer(),
                     type = queue :: stack | queue | keep_old | {mod, module()},
                     initial_state = notify :: notify | passive,
                     detailed_mail = false :: boolean(),
                     heir :: undefined | name(),
                     heir_data :: undefined | term()}).

-export([start_link/1, start_link/2, start_link/3, start_link/4, start_link/5,
        resize/2, resize/3, usage/1, usage/2, usage_detailed/1, usage_detailed/2,
        active/3, notify/1, post/2, post/3,
        post_sync/2, post_sync/3, post_sync/4, give_away/3, give_away/4]).
-export([init/1,
         active_s/3, passive/3, notify/3,
         callback_mode/0, terminate/3, code_change/4]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API Function Definitions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a new buffer process. The implementation can either
%% be a stack or a queue, depending on which messages will be dropped
%% (older ones or newer ones). Note that stack buffers do not guarantee
%% message ordering.
%% The initial state can be either passive or notify, depending on whether
%% the user wants to get notifications of new messages as soon as possible.
-spec start_link(name(), max(), stack | queue | keep_old | {mod, module()}) -> {ok, pid()}.
start_link(Owner, MaxSize, Type) when ?PROCESS_NAME_GUARD(Owner), is_integer(MaxSize), MaxSize > 0 ->
    start_link(Owner, MaxSize, Type, notify).

%% This one is messy because we have two clauses with 4 values, so we look them
%% up based on guards.
-spec start_link(name(), max(), stack | queue | keep_old | {mod, module()}, notify | passive) -> {ok, pid()}.
start_link(Owner, MaxSize, Type, StateName) when ?PROCESS_NAME_GUARD(Owner),
                                              ?POBOX_START_STATE_GUARD(StateName),
                                              is_integer(MaxSize), MaxSize > 0 ->
    gen_statem:start_link(?MODULE, #pobox_opts{owner=Owner, max = MaxSize, type=Type, initial_state=StateName}, []);
start_link(Name, Owner, MaxSize, Type)
  when MaxSize > 0,
    ?PROCESS_NAME_GUARD_WITH_LOCAL_NO_PID(Name),
    ?PROCESS_NAME_GUARD(Owner),
    ?POBOX_BUFFER_TYPE_GUARD(Type) ->
    start_link(Name, Owner, MaxSize, Type, notify).

-spec start_link(name(), name(), max(), stack | queue | keep_old | {mod, module()},
                 'notify'|'passive') -> {ok, pid()}.
start_link(Name, Owner, MaxSize, Type, StateName)
  when MaxSize > 0,
    ?PROCESS_NAME_GUARD_WITH_LOCAL_NO_PID(Name),
    ?PROCESS_NAME_GUARD(Owner),
    ?POBOX_BUFFER_TYPE_GUARD(Type),
    ?POBOX_START_STATE_GUARD(StateName) ->
    gen_statem:start_link(Name, ?MODULE, #pobox_opts{
        name = Name,
        owner = Owner,
        max = MaxSize,
        type = Type,
        initial_state = StateName
    }, []).

default_opts() ->
  #pobox_opts{owner=self(), initial_state=notify, type=queue}.

-spec(start_link(map() | list()) -> {ok, pid()}).
start_link(Opts) when is_list(Opts) ->
  case validate_opts(proplist_to_pobox_opt_with_defaults(Opts)) of
    PoBoxOpts = #pobox_opts{name=undefined} ->
      gen_statem:start_link(?MODULE, PoBoxOpts, []);
    PoBoxOpts = #pobox_opts{name=Name} ->
      gen_statem:start_link(Name, ?MODULE, PoBoxOpts, [])
  end;
start_link(Opts) when is_map(Opts) ->
  start_link(maps:to_list(Opts)).

-spec(start_link(name(), map() | list()) -> {ok, pid()}).
start_link(Name, Opts) when ?PROCESS_NAME_GUARD_WITH_LOCAL_NO_PID(Name), is_list(Opts) ->
  PoBoxOpts = validate_opts(proplist_to_pobox_opt_with_defaults([{name, Name} | Opts])),
  gen_statem:start_link(Name, ?MODULE, PoBoxOpts, []);
start_link(Name, Opts) when ?PROCESS_NAME_GUARD_WITH_LOCAL_NO_PID(Name), is_map(Opts) ->
  start_link(Name, maps:to_list(Opts)).



%% @doc Allows to take a given buffer, and make it larger or smaller.
%% A buffer can be made larger without overhead, but it may take
%% more work to make it smaller given there could be a
%% need to drop messages that would now be considered overflow.
-spec resize(name(), max()) -> ok.
resize(Box, NewMaxSize) when NewMaxSize > 0 ->
    gen_statem:call(Box, {resize, NewMaxSize}).

%% @doc Allows to take a given buffer, and make it larger or smaller.
%% A buffer can be made larger without overhead, but it may take
%% more work to make it smaller given there could be a
%% need to drop messages that would now be considered overflow.
-spec resize(name(), max(), timeout()) -> ok.
resize(Box, NewMaxSize, Timeout) when NewMaxSize > 0 ->
  gen_statem:call(Box, {resize, NewMaxSize}, Timeout).

%% @doc Get the number of items in the PO Box and the capacity.
-spec usage(name()) -> {non_neg_integer(), pos_integer()}.
usage(Box) ->
    gen_statem:call(Box, usage).

%% @doc Get the number of items in the PO Box and the capacity.
-spec usage(name(), timeout()) -> {non_neg_integer(), pos_integer()}.
usage(Box, Timeout) ->
    gen_statem:call(Box, usage, Timeout).

%% @doc Get a detailed usage map: item count and capacity, plus the current total
%% weight and the weight cap. On an unweighted box, each message implicitly weighs 1
%% (so `weight' == `count') and `max_weight' is `infinity'.
-spec usage_detailed(name()) -> #{count := non_neg_integer(), max := pos_integer(),
                                   weight := non_neg_integer(),
                                   max_weight := pos_integer() | infinity}.
usage_detailed(Box) ->
    gen_statem:call(Box, usage_detailed).

%% @doc Get a detailed usage map. See {@link usage_detailed/1}.
-spec usage_detailed(name(), timeout()) -> #{count := non_neg_integer(), max := pos_integer(),
                                             weight := non_neg_integer(),
                                             max_weight := pos_integer() | infinity}.
usage_detailed(Box, Timeout) ->
    gen_statem:call(Box, usage_detailed, Timeout).

%% @doc Forces the buffer into an active state where it will
%% send the data it has accumulated. The fun passed needs to have
%% two arguments: A message, and a term for state. The function can return,
%% for each element, a tuple of the form {Res, NewState}, where `Res' can be:
%% - `{ok, Msg}' to receive the message in the block that gets shipped
%% - `drop' to ignore the message
%% - `skip' to stop removing elements from the stack, and keep them for later.
-spec active(name(), filter(), State::term()) -> ok.
active(Box, Fun, FunState) when is_function(Fun,2) ->
    gen_statem:cast(Box, {active_s, Fun, FunState}).

%% @doc Forces the buffer into its notify state, where it will send a single
%% message alerting the Owner of new messages before going back to the passive
%% state.
-spec notify(name()) -> ok.
notify(Box) ->
    gen_statem:cast(Box, notify).

%% @doc Sends a message to the PO Box, to be buffered.
-spec post(name(), term()) -> ok.
post(Box, Msg) ->
    gen_statem:cast(Box, {post, Msg}).

%% @doc Sends a message to the PO Box with a pre-calculated weight. On a weighted
%% box (started with `max_weight'), the message counts `Weight' toward the weight
%% cap; on an unweighted box the weight is ignored.
-spec post(name(), term(), pos_integer()) -> ok.
post(Box, Msg, Weight) when is_integer(Weight), Weight > 0 ->
    gen_statem:cast(Box, {post, Msg, Weight}).

%% @doc Sends a message to the PO Box, to be buffered. But give additional
%%      feedback about if PO Box is full. This is very useful when combined
%%      with the keep_old buffer type because it tells you the message will
%%      be dropped.
-spec post_sync(name(), term()) -> ok | full.
post_sync(Box, Msg) when ?PROCESS_NAME_GUARD(Box) ->
    gen_statem:call(Box, {post, Msg}).


-spec post_sync(name(), term(), timeout()) -> ok | full.
post_sync(Box, Msg, Timeout) when ?PROCESS_NAME_GUARD(Box) ->
    gen_statem:call(Box, {post, Msg}, Timeout).

%% @doc Sends a weighted message to the PO Box and reports whether it fit. Replies
%% `full' when the message would not fit under the count or weight cap (or is
%% oversized), `ok' otherwise. On an unweighted box the weight is ignored and the
%% reply is the count-based `full'/`ok' of {@link post_sync/3}.
-spec post_sync(name(), term(), pos_integer(), timeout()) -> ok | full.
post_sync(Box, Msg, Weight, Timeout)
  when ?PROCESS_NAME_GUARD(Box), is_integer(Weight), Weight > 0 ->
    gen_statem:call(Box, {post, Msg, Weight}, Timeout).

%% @doc Give away the PO Box ownership to another process. This will send a message in the following form to Dest:
%%      {pobox_transfer, BoxPid :: pid(), PreviousOwnerPid :: pid(), undefined, give_away}
-spec give_away(name(), name(), timeout()) -> boolean().
give_away(Box, Dest, Timeout) when
    ?PROCESS_NAME_GUARD(Box), ?PROCESS_NAME_GUARD(Dest) ->
    give_away(Box, Dest, undefined, Timeout).

%% @doc Give away the PO Box ownership to another process. This will send a message in the following form to Dest:
%%      {pobox_transfer, BoxPid :: pid(), PreviousOwnerPid :: pid(), DestData :: term(), give_away}
-spec give_away(name(), name(), term(), timeout()) -> boolean().
give_away(Box, Dest, DestData, Timeout) when
    ?PROCESS_NAME_GUARD(Box), ?PROCESS_NAME_GUARD(Dest) ->
    gen_statem:call(Box, {give_away, Dest, DestData, self()}, Timeout).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_statem Function Definitions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

callback_mode() ->
    state_functions.

%% @private {Owner, Size, Type, StateName, {heir, Heir, HeirData}}
init(#pobox_opts{
    name = Name0,
    owner = Owner,
    max = MaxSize,
    max_weight = MaxWeight,
    type = Type,
    initial_state = StateName,
    detailed_mail = DetailedMail,
    heir = Heir,
    heir_data = HeirData
}) ->
    Name1 = start_link_name_to_name(Name0),
    erlang:link(OwnerPid = where(Owner)),
    MaybeMonitorRef = case Heir of
        undefined -> undefined;
        HeirName when ?PROCESS_NAME_GUARD(HeirName) ->
            MonitorRef = erlang:monitor(process, OwnerPid),
            erlang:unlink(OwnerPid),
            MonitorRef
    end,
    {ok, StateName, #state{
        buf=buf_new(Type, MaxSize, MaxWeight),
        owner=Owner,
        owner_pid=OwnerPid,
        owner_monitor_ref=MaybeMonitorRef,
        heir=Heir,
        heir_data=HeirData,
        detailed_mail=DetailedMail,
        name=Name1
    }}.

%% @private
active_s(cast, {active_s, Fun, FunState}, S = #state{}) ->
    {next_state, active_s, S#state{filter=Fun, filter_state=FunState}};
active_s(cast, notify, S = #state{}) ->
    {next_state, notify, S#state{filter=undefined, filter_state=undefined}};
active_s(cast, {post, Msg}, S = #state{}) ->
    active_s(cast, {post, Msg, 1}, S);
active_s(cast, {post, Msg, W}, S = #state{buf=Buf}) ->
    NewBuf = insert(Msg, W, Buf),
    send(S#state{buf=NewBuf});
active_s(cast, _Msg, _State) ->
    %% unexpected
    keep_state_and_data;
active_s({call, From}, Msg, Data) ->    
    handle_call(From, Msg, active_s, Data);
active_s(info, Msg, Data) ->    
    handle_info(Msg, active_s, Data).

%% @private
passive(cast, notify, State = #state{buf=Buf}) ->
    case size(Buf) of
        0 -> {next_state, notify, State};
        N when N > 0 -> send_notification(State)
    end;
passive(cast, {active_s, Fun, FunState}, S = #state{buf=Buf}) ->
    NewState = S#state{filter=Fun, filter_state=FunState},
    case size(Buf) of
        0 -> {next_state, active_s, NewState};
        N when N > 0 -> send(NewState)
    end;
passive(cast, {post, Msg}, S = #state{}) ->
    passive(cast, {post, Msg, 1}, S);
passive(cast, {post, Msg, W}, S = #state{buf=Buf}) ->
    {next_state, passive, S#state{buf=insert(Msg, W, Buf)}};
passive(cast, _Msg, _State) ->
    %% unexpected
    keep_state_and_data;
passive({call, From}, Msg, Data) ->    
    handle_call(From, Msg, passive, Data);
passive(info, Msg, Data) ->    
    handle_info(Msg, passive, Data).

%% @private
notify(cast, {active_s, Fun, FunState}, S = #state{buf=Buf}) ->
    NewState = S#state{filter=Fun, filter_state=FunState},
    case size(Buf) of
        0 -> {next_state, active_s, NewState};
        N when N > 0 -> send(NewState)
    end;
notify(cast, notify, S = #state{}) ->
    {next_state, notify, S};
notify(cast, {post, Msg}, S = #state{}) ->
    notify(cast, {post, Msg, 1}, S);
notify(cast, {post, Msg, W}, S = #state{buf=Buf}) ->
    send_notification(S#state{buf=insert(Msg, W, Buf)});
notify(cast, _Msg, _State) ->
    %% unexpected
    keep_state_and_data;
notify({call, From}, Msg, Data) ->    
    handle_call(From, Msg, notify, Data);
notify(info, Msg, Data) ->
    handle_info(Msg, notify, Data).
        

%% @private
handle_call(From, {post, Msg}, StateName, S=#state{buf=#buf{max=Size, size=Size}}) ->
    gen_statem:reply(From, full),
    ?MODULE:StateName(cast, {post, Msg}, S);
handle_call(From, {post, Msg}, StateName, S) ->
    gen_statem:reply(From, ok),
    ?MODULE:StateName(cast, {post, Msg}, S);
handle_call(From, {post, Msg, W}, StateName, S=#state{buf=#buf{max_weight=infinity, max=Max, size=Size}}) ->
    %% unweighted: weight ignored, count-based full (as post_sync/3)
    gen_statem:reply(From, case Size >= Max of true -> full; false -> ok end),
    ?MODULE:StateName(cast, {post, Msg, W}, S);
handle_call(From, {post, Msg, W}, StateName, S=#state{buf=Buf}) ->
    %% weighted: full iff the message would not fit without a drop (incl. oversized)
    gen_statem:reply(From, case fits_after_add(W, Buf) of true -> ok; false -> full end),
    ?MODULE:StateName(cast, {post, Msg, W}, S);
handle_call(From, usage, _State, #state{buf=#buf{size=Size, max=MaxSize}}) ->
    gen_statem:reply(From, {Size, MaxSize}),
    keep_state_and_data;
handle_call(From, usage_detailed, _State, #state{buf=Buf}) ->
    gen_statem:reply(From, buf_usage_map(Buf)),
    keep_state_and_data;
handle_call(From, {resize, NewSize}, _StateName, S=#state{buf=Buf}) ->
    {keep_state, S#state{buf=resize_buf(NewSize,Buf)}, [{reply, From, ok}]};
handle_call(From, {give_away, Dest, DestData, Origin}, _StateName, S0=#state{
    owner_pid=OwnerPid, owner_monitor_ref = MaybeOwnerMonitorRef, name=Name
}) ->
    CanUsePid = case where(Dest) of
        DstPid when is_pid(DstPid) ->
            is_process_alive(DstPid) and (Origin =:= OwnerPid) and (DstPid =/= OwnerPid);
        _ -> false
    end,
    case CanUsePid of
        true ->
          case send_ownership_transfer(OwnerPid, Dest, DestData, Name, give_away) of
            {ok, DestPid} ->
              S1 = case MaybeOwnerMonitorRef of
                undefined ->
                  true = erlang:link(DestPid),
                  true = erlang:unlink(OwnerPid),
                  S0#state{owner=Dest, owner_pid=DestPid};
                OwnerMonitorRef when is_reference(OwnerMonitorRef) ->
                  MaybeDestMonitorRef = erlang:monitor(process, DestPid),
                  true = erlang:demonitor(OwnerMonitorRef),
                  S0#state{owner=Dest, owner_pid=DestPid, owner_monitor_ref=MaybeDestMonitorRef}
              end,
              {next_state, passive, S1, [{reply, From, true}]};
            {error, noproc} ->
              {keep_state, S0, [{reply, From, false}]}
          end;
        false ->
            {keep_state, S0, [{reply, From, false}]}
    end;
handle_call(_From, _Msg, _StateName, _Data) ->
    %% die of starvation, caller!
    keep_state_and_data.

%% @private
handle_info(
    {'DOWN', OwnerMonitorRef, process, OwnerPid, Reason},
    _StateName,
    S=#state{
        owner_pid=OwnerPid, owner_monitor_ref=OwnerMonitorRef, heir=Heir, heir_data=HeirData, name=Name
    }) ->
    case send_ownership_transfer(OwnerPid, Heir, HeirData, Name, Reason) of
        {ok, HeirPid} ->
            erlang:link(HeirPid),
            erlang:demonitor(OwnerMonitorRef),
            {next_state, passive, S#state{
                owner=Heir,
                owner_pid=HeirPid,
                owner_monitor_ref=undefined,
                heir=undefined,
                heir_data=undefined
            }};
        {error, noproc} ->
            {stop, [{heir, noproc}, {owner, Reason}], S}
    end;
handle_info({post, Msg}, StateName, State) ->
    %% We allow anonymous posting and redirect it to the internal form.
    ?MODULE:StateName(cast, {post, Msg}, State);

handle_info(_Info, _StateName, _State) ->
    keep_state_and_data.

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Private Function Definitions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

send(S=#state{buf=Buf, owner_pid=OwnerPid, filter=Fun, filter_state=FilterState,
              detailed_mail=Detailed}) ->
    {Msgs, Count, Dropped, DeliveredW, LostW, NewBuf} = buf_filter(Buf, Fun, FilterState),
    OwnerPid ! mail_msg(Detailed, Msgs, Count, Dropped, DeliveredW, LostW),
    NewState = S#state{buf=NewBuf, filter=undefined, filter_state=undefined},
    {next_state, passive, NewState}.

%% Default mail keeps the {mail, Box, Msgs, Count, Lost} shape. With detailed_mail
%% the last element is a metrics map that also carries the weight figures.
mail_msg(false, Msgs, Count, Dropped, _DeliveredW, _LostW) ->
    {mail, self(), Msgs, Count, Dropped};
mail_msg(true, Msgs, Count, Dropped, DeliveredW, LostW) ->
    {mail, self(), Msgs, #{count => Count, lost => Dropped,
                           weight => DeliveredW, lost_weight => LostW}}.

send_notification(S = #state{owner_pid=OwnerPid}) ->
    OwnerPid ! {mail, self(), new_data},
    {next_state, passive, S}.

%%% Generic buffer ops
-spec buf_new(queue | stack | keep_old | {mod, module()}, max(),
              infinity | pos_integer()) -> buffer().
buf_new(queue, Size, MW) -> #buf{type=queue, max=Size, max_weight=MW, data=queue:new()};
buf_new(stack, Size, MW) -> #buf{type=stack, max=Size, max_weight=MW, data=[]};
buf_new(keep_old, Size, MW) -> #buf{type=keep_old, max=Size, max_weight=MW, data=queue:new()};
buf_new(T={mod, Mod}, Size, MW) -> #buf{type=T, max=Size, max_weight=MW, data=Mod:new()}.

insert(Msg, B=#buf{type=T, max=Size, size=Size, drop=Drop, data=Data}) ->
    B#buf{drop=Drop+1, data=push_drop(T, Msg, Size, Data)};
insert(Msg, B=#buf{type=T, size=Size, data=Data}) ->
    B#buf{size=Size+1, data=push(T, Msg, Data)}.

%% Weighted insert: an unweighted box ignores the weight and uses the count-only
%% fast path above; a weighted box wraps the message as {Weight, Msg} and tracks
%% the running total. (Cap enforcement is added in a later cycle.)
insert(Msg, _W, B=#buf{max_weight=infinity}) ->
    insert(Msg, B);
insert(_Msg, W, B=#buf{max_weight=MW, drop=Drop, drop_weight=DW}) when W > MW ->
    %% Oversized: heavier than the whole cap, can never fit. Reject it (counted as a
    %% drop) without disturbing what is already buffered.
    B#buf{drop=Drop + 1, drop_weight=DW + W};
insert(Msg, W, B=#buf{type=keep_old, drop=Drop, drop_weight=DW}) ->
    %% keep_old keeps the older messages: reject the new one when it doesn't fit,
    %% never dropping what is already buffered.
    case fits_after_add(W, B) of
        true  -> weighted_push(Msg, W, B);
        false -> B#buf{drop=Drop + 1, drop_weight=DW + W}
    end;
insert(Msg, W, B=#buf{}) ->
    %% queue / stack / {mod}: drop from the drop-end to make room, then push. This
    %% keeps the newly-posted message, mirroring each type's count-only overflow.
    weighted_push(Msg, W, make_room(W, B)).

weighted_push(Msg, W, B=#buf{type=T, size=Size, weight=Wt, data=Data}) ->
    B#buf{size=Size+1, weight=Wt+W, data=push(T, {W, Msg}, Data)}.

%% Does one more message of weight W fit under both caps without dropping anything?
fits_after_add(W, #buf{size=Size, max=Max, weight=Weight, max_weight=MW}) ->
    Size < Max andalso Weight + W =< MW.

%% Drop from the buffer type's drop-end (subtracting each dropped element's weight and
%% bumping the drop counter) until one more message of weight W would fit. Stops if the
%% buffer empties (the oversized-message case is rejected before this is reached).
make_room(W, B) ->
    case fits_after_add(W, B) of
        true  -> B;
        false ->
            case B of
                #buf{size=0} ->
                    B;
                #buf{type=T, size=Size, weight=Weight, drop=Drop,
                     drop_weight=DropW, data=Data} ->
                    {{value, {EW, _Msg}}, NewData} = drop_one(T, Data),
                    make_room(W, B#buf{size=Size-1, weight=Weight-EW,
                                       drop=Drop+1, drop_weight=DropW+EW, data=NewData})
            end
    end.

%% Remove one element from a buffer type's drop-end, returning it and the new data.
drop_one(queue, Q)    -> queue:out(Q);      % front / oldest
drop_one(stack, [])   -> {empty, []};
drop_one(stack, [H|T])-> {{value, H}, T};   % head / newest
drop_one(keep_old, Q) -> queue:out_r(Q);    % back / newest (reject-new)
drop_one({mod, Mod}, Data) -> Mod:drop_one(Data).

size(#buf{size=Size}) -> Size.

%% Detailed usage map. On an unweighted box each message weighs 1, so the total
%% weight equals the item count and there is no weight cap; a weighted box reports
%% its tracked total and cap.
buf_usage_map(#buf{size=Size, max=Max, max_weight=infinity}) ->
    #{count => Size, max => Max, weight => Size, max_weight => infinity};
buf_usage_map(#buf{size=Size, max=Max, weight=Weight, max_weight=MW}) ->
    #{count => Size, max => Max, weight => Weight, max_weight => MW}.

resize_buf(NewMax, B=#buf{max=Max}) when Max =< NewMax ->
    B#buf{max=NewMax};
resize_buf(NewMax, B=#buf{type=T, size=Size, drop=Drop, data=Data}) ->
    if Size > NewMax ->
        ToDrop = Size - NewMax,
        B#buf{size=NewMax, max=NewMax, drop=Drop+ToDrop,
              data=drop(T, ToDrop, Size, Data)};
       Size =< NewMax ->
        B#buf{max=NewMax}
    end.

%% Returns {Msgs, Count, Lost, DeliveredWeight, LostWeight, NewBuf}. On an unweighted
%% box each message weighs 1, so delivered weight == count and lost weight == lost.
buf_filter(Buf=#buf{max_weight=infinity, type=T, drop=D, data=Data, size=C}, Fun, State) ->
    {Msgs, Count, Dropped, NewData} = filter(T, Data, Fun, State),
    TotalLost = Dropped + D,
    {Msgs, Count, TotalLost, Count, TotalLost,
     Buf#buf{drop=0, size=C-(Count+Dropped), data=NewData}};
buf_filter(Buf=#buf{type=T, drop=D, drop_weight=DW0, data=Data, size=C, weight=W}, Fun, State) ->
    {Msgs, Count, Dropped, DeliveredW, FilterDroppedW, NewData} = wfilter(T, Data, Fun, State),
    {Msgs, Count, Dropped+D, DeliveredW, FilterDroppedW+DW0,
     Buf#buf{drop=0, drop_weight=0, size=C-(Count+Dropped),
             weight=W-(DeliveredW+FilterDroppedW), data=NewData}}.

filter(T, Data, Fun, State) ->
    filter(T, Data, Fun, State, [], 0, 0).

%% Weighted counterpart of filter/4: elements are stored as {Weight, Msg}; the owner
%% filter fun sees the unwrapped Msg, and we accumulate the weight removed (delivered
%% or dropped) so the running total can be decremented.
wfilter(T, Data, Fun, State) ->
    wfilter(T, Data, Fun, State, [], 0, 0, 0, 0).

wfilter(T, Data, Fun, State, Msgs, Count, Drop, DelW, DropW) ->
    case pop(T, Data) of
        {empty, NewData} ->
            {lists:reverse(Msgs), Count, Drop, DelW, DropW, NewData};
        {{value, {W, Msg}}, NewData} ->
            case Fun(Msg, State) of
                {{ok, Term}, NewState} ->
                    wfilter(T, NewData, Fun, NewState, [Term|Msgs], Count+1, Drop, DelW+W, DropW);
                {drop, NewState} ->
                    wfilter(T, NewData, Fun, NewState, Msgs, Count, Drop+1, DelW, DropW+W);
                skip ->
                    {lists:reverse(Msgs), Count, Drop, DelW, DropW, Data}
            end
    end.

filter(T, Data, Fun, State, Msgs, Count, Drop) ->
    case pop(T, Data) of
        {empty, NewData} ->
            {lists:reverse(Msgs), Count, Drop, NewData};
        {{value,Msg}, NewData} ->
            case Fun(Msg, State) of
                {{ok, Term}, NewState} ->
                    filter(T, NewData, Fun, NewState, [Term|Msgs], Count+1, Drop);
                {drop, NewState} ->
                    filter(T, NewData, Fun, NewState, Msgs, Count, Drop+1);
                skip ->
                    {lists:reverse(Msgs), Count, Drop, Data}
            end
    end.

%% Specific buffer ops
push_drop(T = {mod, Mod}, Msg, Size, Data) ->
  case erlang:function_exported(Mod, push_drop, 2) of
    true  -> Mod:push_drop(Msg, Data);
    false -> push(T, Msg, drop(T, Size, Data))
  end;
push_drop(keep_old, _Msg, _Size, Data) -> Data;
push_drop(T, Msg, Size, Data) -> push(T, Msg, drop(T, Size, Data)).

drop(T, Size, Data) -> drop(T, 1, Size, Data).

drop(_, 0, _Size, Data) -> Data;
drop(queue, 1, _Size, Queue) -> queue:drop(Queue);
drop(stack, 1, _Size, [_|T]) -> T;
drop(keep_old, 1, _Size, Queue) -> queue:drop_r(Queue);
drop(queue, N, Size, Queue) ->
    if Size > N  -> element(2, queue:split(N, Queue));
       Size =< N -> queue:new()
    end;
drop(stack, N, Size, L) ->
    if Size > N  -> lists:nthtail(N, L);
       Size =< N -> []
    end;
drop(keep_old, N, Size, Queue) ->
    if Size > N  -> element(1, queue:split(Size - N, Queue));
       Size =< N -> queue:new()
    end;
drop({mod, Mod}, N, Size, Data) ->
    if Size > N -> Mod:drop(N, Data);
       Size =< N -> Mod:new()
    end.

push(queue, Msg, Q) -> queue:in(Msg, Q);
push(stack, Msg, L) -> [Msg|L];
push(keep_old, Msg, Q) -> queue:in(Msg, Q);
push({mod, Mod}, Msg, Data) ->
  Mod:push(Msg, Data).

pop(queue, Q) -> queue:out(Q);
pop(stack, []) -> {empty, []};
pop(stack, [H|T]) -> {{value,H}, T};
pop(keep_old, Q) -> queue:out(Q);
pop({mod, Mod}, Data) -> Mod:pop(Data).


send_ownership_transfer(_PreviousOwnerPid, undefined, _HeirData, _BoxName, _Reason) -> {error, noproc};
send_ownership_transfer(PreviousOwnerPid, NewOwnerName, HeirData, BoxName, Reason)
    when ?PROCESS_NAME_GUARD_WITH_LOCAL_NO_PID(NewOwnerName), ?PROCESS_NAME_GUARD(BoxName) ->
    case where(NewOwnerName) of
        Pid when is_pid(Pid) ->
            Pid ! {pobox_transfer, self(), PreviousOwnerPid, HeirData, Reason},
            {ok, Pid};
        _ ->
            {error, noproc}
    end;
send_ownership_transfer(PreviousOwnerPid, NewOwnerPid, HeirData, BoxName, Reason)
    when is_pid(NewOwnerPid), ?PROCESS_NAME_GUARD(BoxName) ->
    NewOwnerPid ! {pobox_transfer, self(), PreviousOwnerPid, HeirData, Reason},
    {ok, NewOwnerPid}.

where(Pid) when is_pid(Pid) -> Pid;
where(Name) when is_atom(Name) -> erlang:whereis(Name);
where({global, Name}) -> global:whereis_name(Name);
where({via, Module, Name}) -> Module:whereis_name(Name).

proplist_to_pobox_opt_with_defaults(Opts) when is_list(Opts) ->
    Fields = record_info(fields, pobox_opts),
    [Tag | Values] = tuple_to_list(default_opts()),
    Defaults = lists:zip(Fields, Values),
    L = lists:map(fun ({K,V}) -> proplists:get_value(K, Opts, V) end, Defaults),
    list_to_tuple([Tag | L]).


validate_opts(Opts=#pobox_opts{
    name=Name,
    owner=Owner,
    initial_state=StateName,
    max =MaxSize,
    type=Type,
    heir=Heir
}) when
    is_integer(MaxSize), MaxSize > 0,
    ?POBOX_BUFFER_TYPE_GUARD(Type),
    ?POBOX_START_STATE_GUARD(StateName),
    ?PROCESS_NAME_GUARD(Owner),
    Name =:= undefined orelse ?PROCESS_NAME_GUARD_WITH_LOCAL_NO_PID(Name),
    Heir =:= undefined orelse ?PROCESS_NAME_GUARD(Heir) ->
    Opts;
validate_opts(Opts) ->
    erlang:error(badarg, [Opts]).

%% Normalize name to be used by gen:call gen:cast derived functions etc.
start_link_name_to_name(Name0) ->
    case Name0 of
        {local, Name} -> Name;
        undefined -> self();
        Name -> Name
    end.