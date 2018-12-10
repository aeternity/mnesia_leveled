%%----------------------------------------------------------------
%% Copyright (c) 2018 Aeternity Anstalt
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%----------------------------------------------------------------

%% @doc leveled storage backend for Mnesia.

%% Initialization: register() or register(Alias)
%% Usage: mnesia:create_table(Tab, [{leveled_copies, Nodes}, ...]).

-module(mnesia_leveled).


%% ----------------------------------------------------------------------------
%% BEHAVIOURS
%% ----------------------------------------------------------------------------

-behaviour(mnesia_backend_type).
-behaviour(gen_server).


%% ----------------------------------------------------------------------------
%% EXPORTS
%% ----------------------------------------------------------------------------

%%
%% CONVENIENCE API
%%

-export([test_setup/0, test_select/0]).

-export([register/0,
         register/1,
         default_alias/0]).

%%
%% DEBUG API
%%

-export([show_table/1,
         show_table/2,
         show_table/3,
         fold/6,
         get_ref/2]).

%%
%% BACKEND CALLBACKS
%%

%% backend management
-export([init_backend/0,
         add_aliases/1,
         remove_aliases/1]).

%% schema level callbacks
-export([semantics/2,
	 check_definition/4,
	 create_table/3,
	 load_table/4,
	 close_table/2,
	 sync_close_table/2,
	 delete_table/2,
	 info/3]).

%% table synch calls
-export([sender_init/4,
         sender_handle_info/5,
         receiver_first_message/4,
         receive_data/5,
         receive_done/4]).

%% low-level accessor callbacks.
-export([delete/3,
         first/2,
         fixtable/3,
         insert/3,
         last/2,
         lookup/3,
         match_delete/3,
         next/3,
         prev/3,
         repair_continuation/2,
         select/1,
         select/3,
         select/4,
         slot/3,
         update_counter/4]).

%% Index consistency
-export([index_is_consistent/3,
         is_index_consistent/2]).

%% record and key validation
-export([validate_key/6,
         validate_record/6]).

%% file extension callbacks
-export([real_suffixes/0,
         tmp_suffixes/0]).

%%
%% GEN SERVER CALLBACKS AND CALLS
%%

-export([start_proc/4,
         init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3]).

-export([ix_prefixes/3]).


%% ----------------------------------------------------------------------------
%% DEFINES
%% ----------------------------------------------------------------------------

-define(DATA_BUCKET, <<"d">>).
-define(INFO_BUCKET, <<"i">>).

-define(IX_REV, <<"rk">>).

-define(OBJ_TAG, o).

%% Data and meta data (a.k.a. info) are stored in the same table.
%% This is a table of the first byte in data
%% 0    = before meta data
%% 1    = meta data
%% 2    = before data
%% >= 8 = data

-define(INFO_START, 0).
-define(INFO_TAG, 1).
-define(DATA_START, 2).
-define(BAG_CNT, 32).   % Number of bits used for bag object counter
-define(MAX_BAG, 16#FFFFFFFF).

%% enable debugging messages through mnesia:set_debug_level(debug)
-ifndef(MNESIA_LEVELED_NO_DBG).
-define(dbg(Fmt, Args),
        %% avoid evaluating Args if the message will be dropped anyway
        case mnesia_monitor:get_env(debug) of
            none -> ok;
            verbose -> ok;
            _ -> mnesia_lib:dbg_out("~p:~p: "++(Fmt),[?MODULE,?LINE|Args])
        end).
-else.
-define(dbg(Fmt, Args), ok).
-endif.

%% ----------------------------------------------------------------------------
%% RECORDS
%% ----------------------------------------------------------------------------

-record(sel, {alias,                            % TODO: not used
              tab,
              ref,
              keypat,
              ms,                               % TODO: not used
              compiled_ms,
              limit,
              key_only = false,                 % TODO: not used
              direction = forward}).            % TODO: not used

-record(st, { ets
	    , ref
	    , alias
	    , tab
	    , type
	    , size_warnings			% integer()
	    , maintain_size			% boolean()
	    }).

test_setup() ->
    [
      {del_schema, mnesia:delete_schema([node()])}
    , {create_schema, mnesia:create_schema([node()])}
    , {app_start, application:ensure_all_started(mnesia_leveled)}
    , {mnesia_start, mnesia:start()}
    , {register, register()}
    , {{create_tab,t}, mnesia:create_table(t, [{leveled_copies,[node()]}])}
    , {fill_data, [{Obj, mnesia:dirty_write(Obj)} || Obj <- [{t,1,a},
                                                             {t,2,b},
                                                             {t,3,c}]]}
    ].

test_select() ->
    mnesia:activity(
      transaction,
      fun() ->
              t_sel_loop(mnesia:select(t, [{{t,'_','_'},[],['$_']}], 1, read))
      end).

t_sel_loop({Res,Cont}) ->
    [Res|t_sel_loop(mnesia:select(Cont))];
t_sel_loop('$end_of_table') ->
    [].


%% ----------------------------------------------------------------------------
%% CONVENIENCE API
%% ----------------------------------------------------------------------------

register() ->
    register(default_alias()).

register(Alias) ->
    Module = ?MODULE,
    case mnesia:add_backend_type(Alias, Module) of
        {atomic, ok} ->
            {ok, Alias};
        {aborted, {backend_type_already_exists, _}} ->
            {ok, Alias};
        {aborted, Reason} ->
            {error, Reason}
    end.

default_alias() ->
    leveled_copies.


%% ----------------------------------------------------------------------------
%% DEBUG API
%% ----------------------------------------------------------------------------

%% A debug function that shows the leveled table content
show_table(Tab) ->
    show_table(default_alias(), Tab).

show_table(Alias, Tab) ->
    show_table(Alias, Tab, 100).

show_table(Alias, Tab, _Limit) ->
    {Ref, _Type} = get_ref(Alias, Tab),
    Data = book_data_fold(fun(_, Ke, Ve, Acc) ->
                                  [{data, decode_obj_t(Tab, Ke, Ve)}|Acc]
                          end, [], all, Ref),
    Info = book_info_fold(fun(_, Ke, Ve, Acc) ->
                                  [{info, {decode_key(Ke), decode_val(Ve)}}|Acc]
                          end, [], all, Ref),
    lists:reverse(Info) ++ lists:reverse(Data).

%% PRIVATE

%% ----------------------------------------------------------------------------
%% BACKEND CALLBACKS
%% ----------------------------------------------------------------------------

%% backend management

init_backend() ->
    stick_leveled_dir(),
    application:ensure_all_started(mnesia_leveled),
    ok.

%% Prevent reloading of modules in leveled itself during runtime, since it
%% can lead to inconsistent state in leveled and silent data corruption.
stick_leveled_dir() ->
    case code:which(leveled_bookie) of
        BeamPath when is_list(BeamPath), BeamPath =/= "" ->
            Dir = filename:dirname(BeamPath),
            case code:stick_dir(Dir) of
                ok -> ok;
                error -> warn_stick_dir({error, Dir})
            end;
        Other ->
            warn_stick_dir({not_found, Other})
    end.

warn_stick_dir(Reason) ->
    mnesia_lib:warning("cannot make leveled directory sticky:~n~p~n",
                       [Reason]).

add_aliases(_Aliases) ->
    ok.

remove_aliases(_Aliases) ->
    ok.

%% schema level callbacks

%% This function is used to determine what the plugin supports
%% semantics(Alias, storage)   ->
%%    ram_copies | disc_copies | disc_only_copies  (mandatory)
%% semantics(Alias, types)     ->
%%    [bag | set | ordered_set]                    (mandatory)
%% semantics(Alias, index_fun) ->
%%    fun(Alias, Tab, Pos, Obj) -> [IxValue]       (optional)
%% semantics(Alias, _) ->
%%    undefined.
%%
semantics(_Alias, storage) -> disc_only_copies;
semantics(_Alias, types  ) -> [set, ordered_set, bag];
semantics(_Alias, index_types) -> [ordered];
semantics(_Alias, index_fun) -> fun index_f/4;
semantics(_Alias, _) -> undefined.

is_index_consistent(Alias, {Tab, index, PosInfo}) ->
    case info(Alias, Tab, {index_consistent, PosInfo}) of
        true -> true;
        _ -> false
    end.

index_is_consistent(Alias, {Tab, index, PosInfo}, Bool)
  when is_boolean(Bool) ->
    write_info(Alias, Tab, {index_consistent, PosInfo}, Bool).


%% PRIVATE FUN
index_f(_Alias, _Tab, Pos, Obj) ->
    [element(Pos, Obj)].

ix_prefixes(_Tab, _Pos, Obj) ->
    lists:foldl(
      fun(V, Acc) when is_list(V) ->
              try Pfxs = prefixes(list_to_binary(V)),
                   Pfxs ++ Acc
              catch
                  error:_ ->
                      Acc
              end;
         (V, Acc) when is_binary(V) ->
              Pfxs = prefixes(V),
              Pfxs ++ Acc;
         (_, Acc) ->
              Acc
      end, [], tl(tuple_to_list(Obj))).

prefixes(<<P:3/binary, _/binary>>) ->
    [P];
prefixes(_) ->
    [].

%% For now, only verify that the type is set or ordered_set.
%% set is OK as ordered_set is a kind of set.
check_definition(Alias, Tab, Nodes, Props) ->
    Id = {Alias, Nodes},
    {ok, lists:map(
           fun({type, T} = P) ->
                   if T==set; T==ordered_set; T==bag ->
                           P;
                      true ->
                           mnesia:abort({combine_error,
                                         Tab,
                                         [Id, {type,T}]})
                   end;
              ({user_properties, UPs} = P) ->
                   case lists:keyfind(?MODULE, 1, UPs) of
                       {_, MyPs} when is_list(MyPs) ->
                           validate_my_props(MyPs, Tab),
                           P;
                       {_, _} ->
                           mnesia:abort({badarg,Tab,P});
                       false ->
                           P
                   end;
              (P) -> P
           end, Props)}.

validate_my_props([{maintain_size, Bool} = P|T], Tab) ->
    if is_boolean(Bool) ->
            validate_my_props(T, Tab);
       true ->
            mnesia:abort({badarg,Tab,P})
    end;
validate_my_props([], _) ->
    ok.


%% -> ok | {error, exists}
create_table(_Alias, Tab, _Props) ->
    create_mountpoint(Tab).

load_table(Alias, Tab, _LoadReason, Opts) ->
    Type = proplists:get_value(type, Opts),
    LedUserProps = proplists:get_value(
                     leveled_opts, proplists:get_value(
                                     user_properties, Opts, []), default_open_opts()),
    StorageProps = proplists:get_value(
                     leveled, proplists:get_value(
                                storage_properties, Opts, []), LedUserProps),
    LedOpts = mnesia_leveled_params:lookup(Tab, StorageProps),
    ProcName = proc_name(Alias, Tab),
    case whereis(ProcName) of
        undefined ->
            load_table_(Alias, Tab, Type, LedOpts);
        Pid ->
            gen_server:call(Pid, {load, Alias, Tab, Type, LedOpts}, infinity)
    end.

load_table_(Alias, Tab, Type, LedOpts) ->
    ShutdownTime = proplists:get_value(
                     owner_shutdown_time, LedOpts, 120000),
    case mnesia_ext_sup:start_proc(
           Tab, ?MODULE, start_proc, [Alias,Tab,Type, LedOpts],
           [{shutdown, ShutdownTime}]) of
        {ok, _Pid} ->
            ok;

        %% TODO: This reply is according to the manual, but we dont get it.
        {error, {already_started, _Pid}} ->
            %% TODO: Is it an error if the table already is
            %% loaded. This printout is triggered when running
            %% transform_table on a leveled_table that has indexing.
            ?dbg("ERR: table:~p already loaded pid:~p~n",
                 [Tab, _Pid]),
            ok;

        %% TODO: This reply is not according to the manual, but we get it.
        {error, {{already_started, _Pid}, _Stack}} ->
            %% TODO: Is it an error if the table already is
            %% loaded. This printout is triggered when running
            %% transform_table on a leveled_table that has indexing.
            ?dbg("ERR: table:~p already loaded pid:~p stack:~p~n",
                 [Tab, _Pid, _Stack]),
            ok
    end.

close_table(Alias, Tab) ->
    ?dbg("~p: close_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    if is_atom(Tab) ->
            [close_table(Alias, R)
             || {R, _} <- related_resources(Tab)];
       true ->
            ok
    end,
    close_table_(Alias, Tab).

close_table_(Alias, Tab) ->
    case opt_call(Alias, Tab, close_table) of
        {error, noproc} ->
            ?dbg("~p: close_table_(~p) -> noproc~n",
                 [self(), Tab]),
            ok;
        {ok, _} ->
            ok
    end.

-ifndef(MNESIA_LEVELED_NO_DBG).
pp_stack() ->
    Trace = try throw(true)
            catch
                _:_ ->
                    case erlang:get_stacktrace() of
                        [_|T] -> T;
                        [] -> []
                    end
            end,
    pp_calls(10, Trace).

pp_calls(I, [{M,F,A,Pos} | T]) ->
    Spc = lists:duplicate(I, $\s),
    Pp = fun(Mx,Fx,Ax,Px) ->
                [atom_to_list(Mx),":",atom_to_list(Fx),"/",integer_to_list(Ax),
                 pp_pos(Px)]
        end,
    [Pp(M,F,A,Pos)|[["\n",Spc,Pp(M1,F1,A1,P1)] || {M1,F1,A1,P1} <- T]].

pp_pos([]) -> "";
pp_pos([{file,_},{line,L}]) ->
    [" (", integer_to_list(L), ")"].
-endif.

sync_close_table(Alias, Tab) ->
    ?dbg("~p: sync_close_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    close_table(Alias, Tab).

delete_table(Alias, Tab) ->
    ?dbg("~p: delete_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    delete_table(Alias, Tab, data_mountpoint(Tab)).

delete_table(Alias, Tab, MP) ->
    if is_atom(Tab) ->
            [delete_table(Alias, T, M) || {T,M} <- related_resources(Tab)];
       true ->
            ok
    end,
    case opt_call(Alias, Tab, delete_table) of
        {error, noproc} ->
            do_delete_table(Tab, MP);
        {ok, _} ->
            ok
    end.

do_delete_table(Tab, MP) ->
    assert_proper_mountpoint(Tab, MP),
    destroy_db(MP).


info(_Alias, Tab, memory) ->
    try ets:info(tab_name(icache, Tab), memory)
    catch
        error:_ ->
            0
    end;
info(Alias, Tab, size) ->
    case retrieve_size(Alias, Tab) of
	{ok, Size} ->
	    if Size < 10000 -> ok;
	       true -> size_warning(Alias, Tab)
	    end,
	    Size;
	Error ->
	    Error
    end;
info(_Alias, Tab, Item) ->
    case try_read_info(Tab, Item, undefined) of
        {ok, Value} ->
            Value;
        Error ->
            Error
    end.

retrieve_size(_Alias, Tab) ->
    case try_read_info(Tab, size, 0) of
	{ok, Size} ->
	    {ok, Size};
	Error ->
	    Error
    end.

try_read_info(Tab, Item, Default) ->
    try
        {ok, read_info(Item, Default, tab_name(icache, Tab))}
    catch
        error:Reason ->
            {error, Reason}
    end.

write_info(Alias, Tab, Key, Value) ->
    call(Alias, Tab, {write_info, Key, Value}).

%% table synch calls

%% ===========================================================
%% Table synch protocol
%% Callbacks are
%% Sender side:
%%  1. sender_init(Alias, Tab, RemoteStorage, ReceiverPid) ->
%%        {standard, InitFun, ChunkFun} | {InitFun, ChunkFun} when
%%        InitFun :: fun() -> {Recs, Cont} | '$end_of_table'
%%        ChunkFun :: fun(Cont) -> {Recs, Cont1} | '$end_of_table'
%%
%%       If {standard, I, C} is returned, the standard init message will be
%%       sent to the receiver. Matching on RemoteStorage can reveal if a
%%       different protocol can be used.
%%
%%  2. InitFun() is called
%%  3a. ChunkFun(Cont) is called repeatedly until done
%%  3b. sender_handle_info(Msg, Alias, Tab, ReceiverPid, Cont) ->
%%        {ChunkFun, NewCont}
%%
%% Receiver side:
%% 1. receiver_first_message(SenderPid, Msg, Alias, Tab) ->
%%        {Size::integer(), State}
%% 2. receive_data(Data, Alias, Tab, _Sender, State) ->
%%        {more, NewState} | {{more, Msg}, NewState}
%% 3. receive_done(_Alias, _Tab, _Sender, _State) ->
%%        ok
%%
%% The receiver can communicate with the Sender by returning
%% {{more, Msg}, St} from receive_data/4. The sender will be called through
%% sender_handle_info(Msg, ...), where it can adjust its ChunkFun and
%% Continuation. Note that the message from the receiver is sent once the
%% receive_data/4 function returns. This is slightly different from the
%% normal mnesia table synch, where the receiver acks immediately upon
%% reception of a new chunk, then processes the data.
%%

sender_init(Alias, Tab, _RemoteStorage, _Pid) ->
    %% Need to send a message to the receiver. It will be handled in
    %% receiver_first_message/4 below. There could be a volley of messages...
    {standard,
     fun() ->
             select(Alias, Tab, [{'_',[],['$_']}], 100)
     end,
     chunk_fun()}.

sender_handle_info(_Msg, _Alias, _Tab, _ReceiverPid, Cont) ->
    %% ignore - we don't expect any message from the receiver
    {chunk_fun(), Cont}.

receiver_first_message(_Pid, {first, Size} = _Msg, _Alias, _Tab) ->
    {Size, _State = []}.

receive_data(Data, Alias, Tab, _Sender, State) ->
    [insert(Alias, Tab, Obj) || Obj <- Data],
    {more, State}.

receive_done(_Alias, _Tab, _Sender, _State) ->
    ok.

%% End of table synch protocol
%% ===========================================================

%% PRIVATE

chunk_fun() ->
    fun(Cont) ->
            select(Cont)
    end.

%% low-level accessor callbacks.

delete(Alias, Tab, Key) ->
    opt_call(Alias, Tab, {delete, encode_key(Key)}),
    ok.

first(Alias, Tab) ->
    {Ref, _Type} = get_ref(Alias, Tab),
    FoldAccT = {fun(_Bucket, K, _Acc) ->
                        throw({stop_fold, decode_key(K)})
                end, '$end_of_table'},
    {async, Runner} =
        leveled_bookie:book_keylist(
          Ref, ?OBJ_TAG, ?DATA_BUCKET, {null, null}, FoldAccT),
    run_fold(Runner).

last(Alias, Tab) ->
    {Ref, _Type} = call(Alias, Tab, get_ref),
    FoldAccT = {fun(_Bucket, K, _Acc) ->
                        throw({stop_fold, decode_key(K)})
                end, '$end_of_table'},
    {async, Runner} =
        leveled_bookie:book_indexfold(
          Ref, ?DATA_BUCKET, FoldAccT, {?IX_REV, <<>>, <<255>>}, {false, undefined}),
    run_fold(Runner).


%% Not relevant for an ordered_set
fixtable(_Alias, _Tab, _Bool) ->
    true.

%% To save storage space, we avoid storing the key twice. We replace the key
%% in the record with []. It has to be put back in lookup/3.
insert(Alias, Tab, Obj) ->
    Pos = keypos(Tab),
    EncKey = encode_key(element(Pos, Obj)),
    EncVal = encode_val_p(Pos, Obj),
    call(Alias, Tab, {insert, EncKey, EncVal}).

%% Since we replace the key with [] in the record, we have to put it back
%% into the found record.
lookup(Alias, Tab, Key) ->
    EncK = encode_key(Key),
    {Ref, Type} = call(Alias, Tab, get_ref),
    case Type of
	bag -> lookup_bag(Ref, Key, EncK, keypos(Tab));
	_ ->
	    case leveled_bookie:book_get(Ref, ?DATA_BUCKET, EncK) of
		{ok, EncVal} ->
		    [setelement(keypos(Tab), decode_val(EncVal), Key)];
		_ ->
		    []
	    end
    end.

lookup_bag(Ref, K, EncK, KP) ->
    Sz = byte_size(EncK),
    FoldAccT =
        {fun(_Bucket, Kb, V, Acc) ->
                 case Kb of
                     <<EncK:Sz/binary, _:?BAG_CNT>> ->
                         [setelement(KP, decode_val(V), K)|Acc];
                     _ ->  %% shouldn't happen?
                         throw({stop_fold, Acc})
                 end
         end, []},
    {async, Runner} =
        leveled_bookie:book_objectfold(
          Ref, ?OBJ_TAG, ?DATA_BUCKET, {<<EncK/binary, 0:?BAG_CNT>>,
                                        <<EncK/binary, ?MAX_BAG:?BAG_CNT>>},
          FoldAccT, false),
    lists:reverse(run_fold(Runner)).

match_delete(Alias, Tab, Pat) when is_atom(Pat) ->
    %do_match_delete(Alias, Tab, '_'),
    case is_wild(Pat) of
        true ->
            call(Alias, Tab, clear_table),
            ok;
        false ->
            %% can this happen??
            error(badarg)
    end;
match_delete(Alias, Tab, Pat) when is_tuple(Pat) ->
    KP = keypos(Tab),
    Key = element(KP, Pat),
    case is_wild(Key) of
        true ->
            call(Alias, Tab, clear_table);
        false ->
            call(Alias, Tab, {match_delete, Pat})
    end,
    ok.


next(Alias, Tab, Key) ->
    {Ref, _Type} = get_ref(Alias, Tab),
    EncKey = encode_key(Key),
    FoldAccT = {fun(_Bucket, K, Acc) ->
                        if K > EncKey ->
                                throw({stop_fold, decode_key(K)});
                           true ->
                                Acc
                        end
                end, '$end_of_table'},
    {async, Runner} =
        leveled_bookie:book_keylist(
          Ref, ?OBJ_TAG, ?DATA_BUCKET, {EncKey, null}, FoldAccT),
    run_fold(Runner).

prev(Alias, Tab, Key0) ->
    {Ref, _Type} = call(Alias, Tab, get_ref),
    EncKey = encode_key(Key0),
    RevK = revkey(EncKey),
    FoldAccT = {fun(_Bucket, K, Acc) ->
                        if K < EncKey ->
                                throw({stop_fold, decode_key(K)});
                           true ->
                                Acc
                        end
                end, '$end_of_table'},
    {async, Runner} =
        leveled_bookie:book_indexfold(
          Ref, ?DATA_BUCKET, FoldAccT, {?IX_REV, RevK, <<255>>}, {false, undefined}),
    run_fold(Runner).

repair_continuation(Cont, _Ms) ->
    Cont.

select('$end_of_table') ->
    '$end_of_table';
select(Cont) when is_function(Cont, 0) ->
    Cont().

select(Alias, Tab, Ms) ->
    select(Alias, Tab, Ms, infinity).

select(Alias, Tab, Ms, Limit) when Limit==infinity; is_integer(Limit) ->
    {Ref, Type} = get_ref(Alias, Tab),
    do_select(Ref, Tab, Type, Ms, Limit).

slot(Alias, Tab, Pos) when is_integer(Pos), Pos >= 0 ->
    {Ref, _Type} = get_ref(Alias, Tab),
    FoldAccT = {fun(_Bucket, Ke, Ve, P) ->
                        if P == Pos ->
                                throw({stop_fold, decode_obj_t(Tab, Ke, Ve)});
                           true ->
                                P+1
                        end
                end, 1},
    {async, Runner} =
        leveled_bookie:book_objectfold(
          Ref, ?OBJ_TAG, ?DATA_BUCKET, {null, null}, FoldAccT, false),
    run_fold(Runner);
slot(_, _, _) ->
    error(badarg).


update_counter(Alias, Tab, C, Val) when is_integer(Val) ->
    case call(Alias, Tab, {update_counter, C, Val}) of
        badarg ->
            mnesia:abort(badarg);
        Res ->
            Res
    end.

%% server-side part
do_update_counter(C, Val, Ref) ->
    EncK = encode_key(C),
    case leveled_bookie:book_get(Ref, ?DATA_BUCKET, EncK) of
	{ok, EncVal} ->
	    case decode_val(EncVal) of
		{_, _, Old} = Rec when is_integer(Old) ->
		    Res = Old+Val,
		    leveled_bookie:book_put(Ref, ?DATA_BUCKET, EncK,
                                            encode_val(setelement(3, Rec, Res)),
                                            []),
		    Res;
		_ ->
		    badarg
	    end;
	_ ->
	    badarg
    end.

%% PRIVATE

%% record and key validation

validate_key(_Alias, _Tab, RecName, Arity, Type, _Key) ->
    {RecName, Arity, Type}.

validate_record(_Alias, _Tab, RecName, Arity, Type, _Obj) ->
    {RecName, Arity, Type}.

%% file extension callbacks

%% Extensions for files that are permanent. Needs to be cleaned up
%% e.g. at deleting the schema.
real_suffixes() ->
    [".extled"].

%% Extensions for temporary files. Can be cleaned up when mnesia
%% cleans up other temporary files.
tmp_suffixes() ->
    [].


%% ----------------------------------------------------------------------------
%% GEN SERVER CALLBACKS AND CALLS
%% ----------------------------------------------------------------------------

start_proc(Alias, Tab, Type, LedOpts) ->
    ProcName = proc_name(Alias, Tab),
    gen_server:start_link({local, ProcName}, ?MODULE,
                          {Alias, Tab, Type, LedOpts}, []).

init({Alias, Tab, Type, LedOpts}) ->
    process_flag(trap_exit, true),
    {ok, Ref, Ets} = do_load_table(Tab, LedOpts),
    St = #st{ ets = Ets
	    , ref = Ref
	    , alias = Alias
	    , tab = Tab
	    , type = Type
	    , size_warnings = 0
	    , maintain_size = should_maintain_size(Tab)
	    },
    {ok, recover_size_info(St)}.

do_load_table(Tab, LedOpts) ->
    MPd = data_mountpoint(Tab),
    ?dbg("** Mountpoint: ~p~n ~s~n", [MPd, os:cmd("ls " ++ MPd)]),
    Ets = ets:new(tab_name(icache,Tab), [set, protected, named_table]),
    {ok, Ref} = open_leveled(MPd, LedOpts),
    leveled_to_ets(Ref, Ets),
    {ok, Ref, Ets}.

handle_call({load, Alias, Tab, Type, LedOpts}, _From,
            #st{type = Type, alias = Alias, tab = Tab} = St) ->
    {ok, Ref, Ets} = do_load_table(Tab, LedOpts),
    {reply, ok, St#st{ref = Ref, ets = Ets}};
handle_call(get_ref, _From, #st{ref = Ref, type = Type} = St) ->
    {reply, {Ref, Type}, St};
handle_call({write_info, Key, Value}, _From, #st{} = St) ->
    _ = write_info_(Key, Value, St),
    {reply, ok, St};
handle_call({update_counter, C, Incr}, _From, #st{ref = Ref} = St) ->
    {reply, do_update_counter(C, Incr, Ref), St};
handle_call({insert, Key, Val}, _From, St) ->
    do_insert(Key, Val, St),
    {reply, ok, St};
handle_call({delete, EncKey}, _From, St) ->
    do_delete(EncKey, St),
    {reply, ok, St};
handle_call(clear_table, _From, #st{ets = Ets, tab = Tab, ref = Ref} = St) ->
    MPd = data_mountpoint(Tab),
    ?dbg("Attempting clear_table(~p)~n", [Tab]),
    _ = leveled_close(Ref),
    {ok, NewRef} = destroy_recreate(Tab, MPd, leveled_open_opts(Tab)),
    ets:delete_all_objects(Ets),
    leveled_to_ets(NewRef, Ets),
    {reply, ok, St#st{ref = NewRef}};
handle_call({match_delete, Pat}, _From, #st{} = St) ->
    Res = do_match_delete(Pat, St),
    {reply, Res, St};
handle_call(close_table, _From, #st{ref = Ref, ets = Ets} = St) ->
    _ = leveled_close(Ref),
    ets:delete(Ets),
    {reply, ok, St#st{ref = undefined}};
handle_call(delete_table, _From, #st{tab = T, ref = Ref, ets = Ets} = St) ->
    _ = (catch leveled_bookie:book_destroy(Ref)),
    _ = (catch ets:delete(Ets)),
    do_delete_table(T, data_mountpoint(T)),
    {stop, normal, ok, St#st{ref = undefined}}.

handle_cast(size_warning, #st{tab = T, size_warnings = W} = St) when W < 10 ->
    mnesia_lib:warning("large size retrieved from table: ~p~n", [T]),
    if W =:= 9 ->
            OneHrMs = 60 * 60 * 1000,
            erlang:send_after(OneHrMs, self(), unmute_size_warnings);
       true ->
            ok
    end,
    {noreply, St#st{size_warnings = W + 1}};
handle_cast(size_warning, #st{size_warnings = W} = St) when W >= 10 ->
    {noreply, St#st{size_warnings = W + 1}};
handle_cast(_, St) ->
    {noreply, St}.

handle_info(unmute_size_warnings, #st{tab = T, size_warnings = W} = St) ->
    C = W - 10,
    if C > 0 ->
            mnesia_lib:warning("warnings suppressed~ntable: ~p, count: ~p~n",
                               [T, C]);
       true ->
            ok
    end,
    {noreply, St#st{size_warnings = 0}};
handle_info({'EXIT', _, _} = _EXIT, St) ->
    ?dbg("leveled owner received ~p~n", [_EXIT]),
    {noreply, St};
handle_info(_, St) ->
    {noreply, St}.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, #st{ref = Ref}) ->
    if Ref =/= undefined ->
	    leveled_bookie:book_close(Ref);
       true -> ok
    end,
    ok.


%% ----------------------------------------------------------------------------
%% GEN SERVER PRIVATE
%% ----------------------------------------------------------------------------

leveled_open_opts({Tab, index, {Pos,_}}) ->
    UserProps = mnesia_lib:val({Tab, user_properties}),
    IxOpts = proplists:get_value(leveled_index_opts, UserProps, []),
    PosOpts = proplists:get_value(Pos, IxOpts, []),
    leveled_open_opts_(PosOpts);
leveled_open_opts(Tab) ->
    UserProps = mnesia_lib:val({Tab, user_properties}),
    LedOpts = proplists:get_value(leveled_opts, UserProps, []),
    leveled_open_opts_(LedOpts).

leveled_open_opts_(LedOpts) ->
    lists:foldl(
      fun({K,_} = Item, Acc) ->
              lists:keystore(K, 1, Acc, Item)
      end, default_open_opts(), LedOpts).

default_open_opts() ->
    [{log_level, error}].

destroy_recreate(Tab, MPd, LedOpts) ->
    assert_proper_mountpoint(Tab, MPd),
    ok = destroy_db(MPd),
    open_leveled(MPd, LedOpts).

open_leveled(MPd, LedOpts) ->
    open_leveled_(MPd, leveled_open_opts_(LedOpts)).

open_leveled_(MPd, Opts) ->
    {ok, Ref} = leveled_bookie:book_start([{root_path, MPd} | Opts]),
    ?dbg("~p: Open - Leveled: ~s~n  -> {ok, ~p}~n",
         [self(), MPd, Ref]),
    {ok, Ref}.

leveled_close(undefined) ->
    ok;
leveled_close(Ref) ->
    Res = leveled_bookie:book_close(Ref),
    erlang:garbage_collect(),
    Res.

destroy_db(MPd) ->
    [_|_] = MPd, % ensure MPd is non-empty
    _RmRes = os:cmd("rm -rf " ++ MPd ++ "/*"),
    ?dbg("~p: RmRes = '~s'~n", [self(), _RmRes]),
    ok.

leveled_to_ets(Ref, Ets) ->
    Info = book_info_fold(
             fun(_, Ke, Ve, Acc) ->
                     K = decode_key(Ke),
                     V = decode_val(Ve),
                     [{{info,K}, V}|Acc]
             end, [], all, Ref),
    ets:insert(Ets, Info).

opt_call(Alias, Tab, Req) ->
    ProcName = proc_name(Alias, Tab),
    case whereis(ProcName) of
        undefined ->
            ?dbg("proc_name(~p, ~p): ~p; NO PROCESS~n",
                 [Alias, Tab, ProcName]),
            {error, noproc};
        Pid when is_pid(Pid) ->
            ?dbg("proc_name(~p, ~p): ~p; Pid = ~p~n",
                 [Alias, Tab, ProcName, Pid]),
            {ok, gen_server:call(Pid, Req, infinity)}
    end.

call(Alias, Tab, Req) ->
    ProcName = proc_name(Alias, Tab),
    case gen_server:call(ProcName, Req, infinity) of
        badarg ->
            mnesia:abort(badarg);
        {abort, _} = Err ->
            mnesia:abort(Err);
        Reply ->
            Reply
    end.

size_warning(Alias, Tab) ->
    ProcName = proc_name(Alias, Tab),
    gen_server:cast(ProcName, size_warning).

%% server-side end of insert/3.
do_insert(K, V, #st{type = bag} = St) ->
    do_insert_bag(K, V, St),
    ok;
do_insert(K, V, #st{ref = Ref, maintain_size = false}) ->
    %% book_put(Pid, Bucket, Key, Object, IndexSpecs)
    book_put(Ref, K, V);
do_insert(K, V, #st{ref = Ref, maintain_size = true} = St) ->
    IsNew =
	case leveled_bookie:book_get(Ref, ?DATA_BUCKET, K) of
	    {ok, _} ->
		false;
	    not_found ->
		true
	end,
    case IsNew of
	true ->
            book_add(K, V, St),
            ok;
	false ->
            book_put(Ref, K, V)
    end,
    ok.

do_insert_bag(K, V, #st{} = St) ->
    KSz = byte_size(K),
    do_insert_bag_(KSz, K, V, St).


%% There's a potential access pattern that would force counters to
%% creep upwards and eventually hit the limit. This could be addressed,
%% with compaction. TODO.
do_insert_bag_(Sz, K, V, #st{ref = Ref} = St) ->
    Range = {<<K:Sz/binary, 0:?BAG_CNT>>, <<K:Sz/binary, ?MAX_BAG:?BAG_CNT>>},
    F = fun(_, _K1, V1, _Sz1) when V1 =:= V ->
                throw({stop_fold, exists});
           (_, K1, _V1, Sz1) ->
                case K1 of
                    <<K:Sz/binary, N:?BAG_CNT>> ->
                        N;
                    _ ->
                        Sz1
                end
        end,
    case book_data_fold(F, 0, Range, Ref) of
        exists ->
            St;
        0 ->
            %% no matching key
            book_add(<<K/binary, 1:?BAG_CNT>>, V, bag_revkey(K, 1), St);
        LastN ->
            NewN = LastN+1,
            book_add(<<K/binary, NewN:?BAG_CNT>>, V,
                     bag_revkey(K, NewN), St)
    end.

book_add(K, V, St) ->
    book_add(K, V, revkey(K), St).

book_add(K, V, RevK, #st{ref = Ref, maintain_size = false} = St) ->
    leveled_bookie:book_put(Ref, ?DATA_BUCKET, K, V, [ix_add(RevK)]),
    St;
book_add(K, V, RevK, #st{ref = Ref, ets = Ets, maintain_size = true} = St) ->
    CurSz = read_info(size, 0, Ets),
    NewSz = CurSz + 1,
    leveled_bookie:book_put(Ref, ?DATA_BUCKET, K, V, [ix_add(RevK)]),
    update_size(NewSz, St).

update_size(Sz, #st{ref = Ref, ets = Ets} = St) ->
    {Ki, Vi} = info_obj(size, Sz),
    leveled_bookie:book_put(Ref, ?INFO_BUCKET, Ki, Vi, []),
    ets_insert_info(Ets, size, Sz),
    St.
    

book_put(Ref, K, V) ->
    RevK = revkey(K),
    leveled_bookie:book_put(Ref, ?DATA_BUCKET, K, V, [ix_add(RevK)]).

book_put(Ref, Bucket, K, V, Ix) ->
    leveled_bookie:book_put(Ref, Bucket, K, V, Ix).

book_delete(Ref, Bucket, K, Ix) ->
    leveled_bookie:book_delete(Ref, Bucket, K, Ix).

ix_add(RevK) ->
    {add, ?IX_REV, RevK}.

ix_rm(bag, K) ->
    Sz = bit_size(K),
    SzK = Sz-?BAG_CNT,
    <<Ks:SzK/bitstring, N:?BAG_CNT>> = K,
    ix_rm(bag_revkey(Ks, N));
ix_rm(_, K) ->
    ix_rm(revkey(K)).

ix_rm(RevK) ->
    {remove, ?IX_REV, RevK}.

revkey(K) ->
    sext:reverse_sext(K).

bag_revkey(K, N) ->
    RevK = revkey(K),
    <<RevK/binary, (?MAX_BAG - N):?BAG_CNT>>.

book_delete_existing_keys(Keys, #st{ref = Ref, ets = Ets, type = Type,
                                    maintain_size = MSz} = St) ->
    case MSz of
        true ->
            CurSz = read_info(size, 0, Ets),
            NewSz = erlang:max(0, CurSz - length(Keys)),
            {Ki, Vi} = info_obj(size, NewSz),
            [book_delete(Ref, ?DATA_BUCKET, K, [ix_rm(Type, K)])
             || K <- Keys],
            book_put(Ref, ?INFO_BUCKET, Ki, Vi, []),
            ets_insert_info(Ets, size, NewSz);
        false ->
            [book_delete(Ref, ?DATA_BUCKET, K, [ix_rm(Type, K)])
             || K <- Keys]
    end,
    St.

%% server-side part
do_delete(Key, #st{type = bag} = St) ->
    do_delete_bag(byte_size(Key), Key, St),
    ok;
do_delete(Key, #st{ref = Ref, maintain_size = false}) ->
    book_delete(Ref, ?DATA_BUCKET, Key, [ix_rm(revkey(Key))]);
do_delete(Key, #st{ets = Ets, ref = Ref, maintain_size = true}) ->
    CurSz = read_info(size, 0, Ets),
    case leveled_bookie:book_get(Ref, ?DATA_BUCKET, Key) of
	{ok, _} ->
	    NewSz = CurSz -1,
	    {Ki, Vi} = info_obj(size, NewSz),
            book_delete(Ref, ?DATA_BUCKET, Key, [ix_rm(revkey(Key))]),
            book_put(Ref, ?INFO_BUCKET, Ki, Vi, []),
	    ets_insert_info(Ets, size, NewSz);
	not_found ->
	    false
    end.

do_delete_bag(Sz, Key, #st{ref = Ref} = St) ->
    Range = {<<Key:Sz/binary, 0:?BAG_CNT>>, <<Key:Sz/binary, ?MAX_BAG:?BAG_CNT>>},
    F = fun(_, K1, _V1, Acc) ->
                case K1 of
                    <<Key:Sz/binary, _:?BAG_CNT>> ->
                        [K1|Acc];
                    _ ->
                        throw({stop_fold, Acc})
                end
        end,
    case book_data_fold(F, [], Range, Ref) of
        [] ->
            St;
        [_|_] = Keys ->
            book_delete_existing_keys(Keys, St)
    end.

book_data_fold(F, Acc, Range, Ref) ->
    book_fold(F, Acc, ?DATA_BUCKET, Range, Ref).

book_info_fold(F, Acc, Range, Ref) ->
    book_fold(F, Acc, ?INFO_BUCKET, Range, Ref).

book_fold(F, Acc, Bucket, Range, Ref) ->
    {async, Folder} =
        leveled_bookie:book_objectfold(
          Ref, ?OBJ_TAG, Bucket, Range, {F, Acc}, false),
    run_fold(Folder).

run_fold(Folder) ->
    try Folder()
    catch
        throw:{stop_fold, Res} ->
            Res
    end.

do_match_delete(Pat, #st{ref = Ref, tab = Tab, type = Type} = St) ->
    Fun = fun(_, Key, Acc) -> [Key|Acc] end,
    Keys = do_fold(Ref, Tab, Type, Fun, [], [{Pat,[],['$_']}], 30),
    case Keys of
	[] ->
	    ok;
	_ ->
            book_delete_existing_keys(Keys, St),
	    ok
    end.

recover_size_info(#st{ ref = Ref
		     , maintain_size = MaintainSize
		     } = St) ->
    case MaintainSize of
        true ->
            %% info initialized by leveled_to_ets/2
            Sz = book_data_fold(fun(_,_,_,Acc) -> Acc+1 end, 0, {null,null}, Ref),
            update_size(Sz, St),
            St;
        false ->
            %% size is not maintained, ensure it's marked accordingly
            delete_info_(size, St),
            St
    end.

should_maintain_size(Tab) ->
    my_property(Tab, maintain_size, false).

my_property(Tab, Prop, Default) ->
    proplists:get_value(Prop, property(Tab, ?MODULE, []), Default).

property(Tab, Prop, Default) ->
    try mnesia:read_table_property(Tab, Prop) of
        {Prop, P} ->
            P
    catch
        error:_ -> Default;
        exit:_  -> Default
    end.

write_info_(Item, Val, #st{ets = Ets, ref = Ref}) ->
    leveled_insert_info(Ref, Item, Val),
    ets_insert_info(Ets, Item, Val).

ets_insert_info(Ets, Item, Val) ->
    ets:insert(Ets, {{info, Item}, Val}).

ets_delete_info(Ets, Item) ->
    ets:delete(Ets, {info, Item}).

leveled_insert_info(Ref, Item, Val) ->
    EncKey = encode_key(Item),
    EncVal = encode_val(Val),
    leveled_bookie:book_put(Ref, ?INFO_BUCKET, EncKey, EncVal, []).

leveled_delete_info(Ref, Item) ->
    EncKey = encode_key(Item),
    leveled_bookie:book_delete(Ref, ?INFO_BUCKET, EncKey, []).

info_obj(Item, Val) ->
    {encode_key(Item), encode_val(Val)}.

delete_info_(Item, #st{ets = Ets, ref = Ref}) ->
    leveled_delete_info(Ref, Item),
    ets_delete_info(Ets, Item).

read_info(Item, Default, Ets) ->
    case ets:lookup(Ets, {info,Item}) of
        [] ->
            Default;
        [{_,Val}] ->
            Val
    end.

tab_name(icache, Tab) ->
    list_to_atom("mnesia_ext_icache_" ++ tabname(Tab)).

proc_name(_Alias, Tab) ->
    list_to_atom("mnesia_ext_proc_" ++ tabname(Tab)).


%% ----------------------------------------------------------------------------
%% PRIVATE SELECT MACHINERY
%% ----------------------------------------------------------------------------

do_select(Ref, Tab, Type, MS, Limit) ->
    do_select(Ref, Tab, Type, MS, false, Limit).

do_select(Ref, Tab, _Type, MS, AccKeys, Limit) when is_boolean(AccKeys) ->
    Keypat = keypat(MS, keypos(Tab)),
    Sel = #sel{tab = Tab,
               ref = Ref,
               keypat = Keypat,
               ms = MS,
               compiled_ms = ets:match_spec_compile(MS),
               key_only = needs_key_only(MS),
               limit = Limit},
    do_select_(Ref, Sel, AccKeys, []).

do_select_(Ref, #sel{keypat = Pfx,
                     compiled_ms = MS,
                     limit = Limit} = Sel, AccKeys, Acc) ->
    StartKey = case {Acc, Pfx} of
                   {{'$sel_cont', K}, _} -> K;
                   {_, <<>>}             -> null;
                   _                     -> Pfx
               end,
    F = fun(_, K, V, Acc1) ->
                select_traverse(K, V, Limit, Pfx, MS, Sel, AccKeys, Acc1)
        end,
    case book_data_fold(F, Acc, {StartKey, null}, Ref) of
        L when is_list(L), Limit==infinity ->
            lists:reverse(L);
        {L, '$end_of_table'} when Limit==infinity ->
            L;
        [] when is_integer(Limit) ->
            '$end_of_table';
        L when is_list(L), is_integer(Limit) ->
            {lists:reverse(L), '$end_of_table'};
        {_,_} = Res ->
            Res
    end.

needs_key_only([{HP,_,Body}]) ->
    BodyVars = lists:flatmap(fun extract_vars/1, Body),
    %% Note that we express the conditions for "needs more than key" and negate.
    not(wild_in_body(BodyVars) orelse
        case bound_in_headpat(HP) of
            {all,V} -> lists:member(V, BodyVars);
            Vars    -> any_in_body(lists:keydelete(2,1,Vars), BodyVars)
        end);
needs_key_only(_) ->
    %% don't know
    false.

extract_vars([H|T]) ->
    extract_vars(H) ++ extract_vars(T);
extract_vars(T) when is_tuple(T) ->
    extract_vars(tuple_to_list(T));
extract_vars(T) when T=='$$'; T=='$_' ->
    [T];
extract_vars(T) when is_atom(T) ->
    case is_wild(T) of
        true ->
            [T];
        false ->
            []
    end;
extract_vars(_) ->
    [].

any_in_body(Vars, BodyVars) ->
    lists:any(fun({_,Vs}) ->
                      intersection(Vs, BodyVars) =/= []
              end, Vars).

intersection(A,B) when is_list(A), is_list(B) ->
    A -- (A -- B).

wild_in_body(BodyVars) ->
    intersection(BodyVars, ['$$','$_']) =/= [].

bound_in_headpat(HP) when is_atom(HP) ->
    {all, HP};
bound_in_headpat(HP) when is_tuple(HP) ->
    [_|T] = tuple_to_list(HP),
    map_vars(T, 2).

map_vars([H|T], P) ->
    case extract_vars(H) of
        [] ->
            map_vars(T, P+1);
        Vs ->
            [{P, Vs}|map_vars(T, P+1)]
    end;
map_vars([], _) ->
    [].

select_traverse(K, _V, _Limit, _Pfx, _MS, _Sel, _AccKeys, {'$sel_cont', K}) ->
    [];
select_traverse(K, V, Limit, Pfx, MS, #sel{tab = Tab} = Sel, AccKeys, Acc) ->
    case is_prefix(Pfx, K) of
	true ->
	    Rec = setelement(keypos(Tab), decode_val(V), decode_key(K)),
	    case ets:match_spec_run([Rec], MS) of
		[] -> Acc;
		[Match] ->
                    Acc1 = if AccKeys ->
                                   [{K, Match}|Acc];
                              true ->
                                   [Match|Acc]
                           end,
                    traverse_continue(K, decr(Limit), Sel, AccKeys, Acc1)
            end;
        false ->
            throw({stop_fold, {lists:reverse(Acc), '$end_of_table'}})
    end.

is_prefix(A, B) when is_binary(A), is_binary(B) ->
    Sa = byte_size(A),
    case B of
        <<A:Sa/binary, _/binary>> ->
            true;
        _ ->
            false
    end.

decr(I) when is_integer(I) ->
    I-1;
decr(infinity) ->
    infinity.

traverse_continue(K, 0, #sel{ref = Ref} = Sel, AccKeys, Acc) ->
    throw({stop_fold,
           {lists:reverse(Acc),
            fun() ->
                    do_select_(Ref, Sel, AccKeys, {'$sel_cont', K})
            end}});
traverse_continue(_K, _Limit, _Sel, _AccKeys, Acc) ->
    Acc.

keypat([H|T], KeyPos) ->
    keypat(T, KeyPos, keypat_pfx(H, KeyPos)).

keypat(_, _, <<>>) -> <<>>;
keypat([H|T], KeyPos, Pfx0) ->
    Pfx = keypat_pfx(H, KeyPos),
    keypat(T, KeyPos, common_prefix(Pfx, Pfx0));
keypat([], _, Pfx) ->
    Pfx.

common_prefix(<<H, T/binary>>, <<H, T1/binary>>) ->
    <<H, (common_prefix(T, T1))/binary>>;
common_prefix(_, _) ->
    <<>>.

keypat_pfx({HeadPat,_Gs,_}, KeyPos) when is_tuple(HeadPat) ->
    KP      = element(KeyPos, HeadPat),
    sext:prefix(KP);
keypat_pfx(_, _) ->
    <<>>.

%% ----------------------------------------------------------------------------
%% COMMON PRIVATE
%% ----------------------------------------------------------------------------

%% Note that since a callback can be used as an indexing backend, we
%% cannot assume that keypos will always be 2. For indexes, the tab
%% name will be {Tab, index, Pos}, and The object structure will be
%% {{IxKey,Key}} for an ordered_set index, and {IxKey,Key} for a bag
%% index.
%%
keypos({_, index, _}) ->
    1;
keypos({_, retainer, _}) ->
    2;
keypos(Tab) when is_atom(Tab) ->
    2.

encode_key(Key) ->
    sext:encode(Key).

decode_key(CodedKey) ->
    case sext:partial_decode(CodedKey) of
        {full, Result, _} ->
            Result;
        _Other ->
            lager:error("Unexpected: sext:partial_decode(~p) -> ~p~n", [CodedKey, _Other]),
            error(badarg, CodedKey)
    end.

%% encode_val_t(Tab, Val) ->
%%     encode_val_p(keypos(Tab), Val).

encode_val_p(P, Val) ->
    encode_val(setelement(P, Val, [])).

encode_val(Val) ->
    term_to_binary(Val).

decode_val(CodedVal) ->
    binary_to_term(CodedVal).

create_mountpoint(Tab) ->
    MPd = data_mountpoint(Tab),
    case filelib:is_dir(MPd) of
        false ->
            file:make_dir(MPd),
            ok;
        true ->
            Dir = mnesia_lib:dir(),
            case lists:prefix(Dir, MPd) of
                true ->
                    ok;
                false ->
                    {error, exists}
            end
    end.

assert_proper_mountpoint(_Tab, _MPd) ->
    %% TODO: not yet implemented. How to verify that the MPd var points
    %% to the directory we actually want deleted?
    ok.

data_mountpoint(Tab) ->
    Dir = mnesia_monitor:get_env(dir),
    filename:join(Dir, tabname(Tab) ++ ".extled").

tabname({Tab, index, {{Pos},_}}) ->
    atom_to_list(Tab) ++ "-=" ++ atom_to_list(Pos) ++ "=-_ix";
tabname({Tab, index, {Pos,_}}) ->
    atom_to_list(Tab) ++ "-" ++ integer_to_list(Pos) ++ "-_ix";
tabname({Tab, retainer, Name}) ->
    atom_to_list(Tab) ++ "-" ++ retainername(Name) ++ "-_RET";
tabname(Tab) when is_atom(Tab) ->
    atom_to_list(Tab) ++ "-_tab".

retainername(Name) when is_atom(Name) ->
    atom_to_list(Name);
retainername(Name) when is_list(Name) ->
    try binary_to_list(list_to_binary(Name))
    catch
        error:_ ->
            lists:flatten(io_lib:write(Name))
    end;
retainername(Name) ->
    lists:flatten(io_lib:write(Name)).

related_resources(Tab) ->
    TabS = atom_to_list(Tab),
    Dir = mnesia_monitor:get_env(dir),
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:flatmap(
              fun(F) ->
                      Full = filename:join(Dir, F),
                      case is_index_dir(F, TabS) of
                          false ->
                              case is_retainer_dir(F, TabS) of
                                  false ->
                                      [];
                                  {true, Name} ->
                                      [{{Tab, retainer, Name}, Full}]
                              end;
                          {true, Pos} ->
                              [{{Tab, index, {Pos,ordered}}, Full}]
                      end
              end, Files);
        _ ->
            []
    end.

is_index_dir(F, TabS) ->
    case re:run(F, TabS ++ "-([0-9]+)-_ix.extled", [{capture, [1], list}]) of
        nomatch ->
            false;
        {match, [P]} ->
            {true, list_to_integer(P)}
    end.

is_retainer_dir(F, TabS) ->
    case re:run(F, TabS ++ "-(.+)-_RET", [{capture, [1], list}]) of
        nomatch ->
            false;
        {match, [Name]} ->
            {true, Name}
    end.

get_ref(Alias, Tab) ->
    call(Alias, Tab, get_ref).

fold(Alias, Tab, Fun, Acc, MS, N) ->
    {Ref, Type} = get_ref(Alias, Tab),
    do_fold(Ref, Tab, Type, Fun, Acc, MS, N).

%% can be run on the server side.
do_fold(Ref, Tab, Type, Fun, Acc, MS, N) ->
    {AccKeys, F} =
        if is_function(Fun, 3) ->
                {true, fun({K,Obj}, Acc1) ->
                               Fun(Obj, K, Acc1)
                       end};
           is_function(Fun, 2) ->
                {false, Fun}
        end,
    do_fold1(do_select(Ref, Tab, Type, MS, AccKeys, N), F, Acc).

do_fold1('$end_of_table', _, Acc) ->
    Acc;
do_fold1({L, Cont}, Fun, Acc) ->
    Acc1 = lists:foldl(Fun, Acc, L),
    do_fold1(select(Cont), Fun, Acc1).

decode_obj_t(Tab, K, V) ->
    decode_obj_p(keypos(Tab), K, V).

decode_obj_p(P, K, V) ->
    setelement(P, decode_val(V), decode_key(K)).

is_wild('_') ->
    true;
is_wild(A) when is_atom(A) ->
    case atom_to_list(A) of
        "\$" ++ S ->
            try begin
                    _ = list_to_integer(S),
                    true
                end
            catch
                error:_ ->
                    false
            end;
        _ ->
            false
    end;
is_wild(_) ->
    false.
