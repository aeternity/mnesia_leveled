%%----------------------------------------------------------------
%% Copyright (c) 2013-2016 Klarna AB
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

-module(mnesia_leveled_fallback).

-export([run/0]).

-define(m(A,B), fun() -> L = ?LINE,
                         case {A,B} of
                             {__X, __X} ->
                                 B;
                             Other ->
                                 error({badmatch, [Other,
                                                   {line, L}]})
                         end
                end()).

run() ->
    cleanup(),
    mnesia_leveled_tlib:start_mnesia(reset),
    mnesia_leveled_tlib:create_table(led),
    ok = mnesia:backup("bup0.BUP"),
    [mnesia:dirty_write({t,K,V}) || {K,V} <- [{a,1},
                                              {b,2},
                                              {c,3}]],
    ok = mnesia:backup("bup1.BUP"),
    [mnesia:dirty_write({t,K,V}) || {K,V} <- [{d,4},
                                              {e,5},
                                              {f,6}]],
    ok = mnesia:backup("bup2.BUP"),
    ct:log("*****************************************~n", []),
    load_backup("bup0.BUP"),
    ?m([], mnesia:dirty_match_object(t, {t,'_','_'})),
    ?m([], mnesia:dirty_index_read(t,2,v)),
    ct:log("*****************************************~n", []),
    load_backup("bup1.BUP"),
    ?m([{t,a,1},{t,b,2},{t,c,3}], mnesia:dirty_match_object(t, {t,'_','_'})),
    ?m([{t,b,2}], mnesia:dirty_index_read(t,2,v)),
    ct:log("*****************************************~n", []),
    load_backup("bup2.BUP"),
    ?m([{t,a,1},{t,b,2},{t,c,3},
        {t,d,4},{t,e,5},{t,f,6}], mnesia:dirty_match_object(t, {t,'_','_'})),
    ?m([{t,b,2}], mnesia:dirty_index_read(t,2,v)),
    ?m([{t,e,5}], mnesia:dirty_index_read(t,5,v)),
    ok.

load_backup(BUP) ->
    mnesia_leveled_tlib:trace(
      fun() ->
              ct:log("loading backup ~s~n", [BUP]),
              ok = mnesia:install_fallback(BUP),
              ct:log("stopping~n", []),
              mnesia:stop(),
              timer:sleep(3000),
              ct:log("starting~n", []),
              mnesia:start(),
              WaitRes = mnesia:wait_for_tables([t], 5000),
              ct:log("WaitRes = ~p~n", [WaitRes])
      end,
      mods(0)
     ).

cleanup() ->
    os:cmd("rm *.BUP").

mods(0) ->
    [];
mods(1) ->
    [
     {l, mnesia_leveled},
     {g, leveled}
    ];
mods(2) ->
    [
     %% {l, mnesia_monitor},
     {g, mnesia_leveled},
     {l, mnesia_bup},
     {g, mnesia_lib},
     {g, mnesia_schema},
     %% {g, mnesia_loader},
     {g, mnesia_index},
     {l, mnesia_tm}
    ].
