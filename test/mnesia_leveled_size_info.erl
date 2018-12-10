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

-module(mnesia_leveled_size_info).

-export([run/0]).

-define(m(A, B), (fun(L, true ) -> {L,A} = {L,B};
                     (L, false) -> {L,0} = {L,B} end)(?LINE, Bool)).


run() ->
    initialize_mnesia(),
    test_set(s0, false),
    test_set(sf, false),
    test_set(st, true),
    test_bag(b0, false),
    test_bag(bf, false),
    test_bag(bt, true).

initialize_mnesia() ->
    mnesia:stop(),
    mnesia:delete_schema([node()]),
    mnesia:create_schema([node()], [{backend_types,
                                     [{leveled_copies, mnesia_leveled}]}]),
    mnesia:start(),
    {atomic,ok} = new_tab(s0, set, none),
    {atomic,ok} = new_tab(sf, set, false),
    {atomic,ok} = new_tab(st, set, true),
    {atomic,ok} = new_tab(b0, bag, none),
    {atomic,ok} = new_tab(bf, bag, false),
    {atomic,ok} = new_tab(bt, bag, true),
    ok.

new_tab(Name, Type, MaintainSize) ->
    Props = user_props(MaintainSize),
    mnesia:create_table(Name, [{type, Type},
                               {record_name, x},
                               {leveled_copies, [node()]},
                               {user_properties, Props}
                              ]).

user_props(none) ->
    [];
user_props(Bool) when is_boolean(Bool) ->
    [{mnesia_leveled,
      [{maintain_size, Bool}]
     }].

test_set(T, Bool) ->
    await(T),
    ?m(0, mnesia:table_info(T, size)),
    ?m(1, w(T, 1, a)),
    ?m(1, w(T, 1, b)),
    ?m(2, w(T, 2, c)),
    ?m(3, w(T, 3, d)),
    ?m(2, d(T, 3)),
    mnesia:stop(),
    mnesia:start(),
    await(T),
    ?m(2, mnesia:table_info(T, size)).

test_bag(T, Bool) ->
    await(T),
    ?m(0, mnesia:table_info(T, size)),
    ?m(1, w(T, 1, a)),
    ?m(2, w(T, 1, b)),
    ?m(3, w(T, 2, a)),
    ?m(4, w(T, 2, d)),
    ?m(5, w(T, 2, c)),
    ?m(4, do(T, 2, c)),
    ?m(2, d(T, 2)),
    mnesia:stop(),
    mnesia:start(),
    await(T),
    ?m(2, mnesia:table_info(T, size)).

w(T, K, V) ->
    ok = mnesia:dirty_write(T, {x, K, V}),
    mnesia:table_info(T, size).

d(T, K) ->
    mnesia:dirty_delete({T, K}),
    mnesia:table_info(T, size).

do(T, K, V) ->
    mnesia:dirty_delete_object(T, {x, K, V}),
    mnesia:table_info(T, size).

await(T) ->
    Bool = true,
    ?m(ok, mnesia:wait_for_tables([T], 10000)).
