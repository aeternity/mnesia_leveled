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

-module(mnesia_leveled_tlib).

-export([start_mnesia/0,
         start_mnesia/1,
         create_table/1,
         create_table/3,
         trace/2]).


start_mnesia() ->
    start_mnesia(false).

start_mnesia(Mode) ->
    if Mode==reset ->
            mnesia:stop(),
            mnesia:delete_schema([node()]),
            mnesia:create_schema([node()],
                                 [{backend_types,
                                   [{led,mnesia_leveled}]}]);
       true -> ok
    end,
    mnesia:start().

create_table(Backend) ->
    create_table(Backend, [k,v], [v]).

create_table(Backend, Attrs, Indexes) ->
    mnesia:create_table(t, [{index,Indexes}, {attributes,Attrs},
                            {Backend, [node()]}]).

trace(F, Ms) ->
    dbg:tracer(),
    [tp(M) || M <- Ms],
    dbg:p(all,[c]),
    try F()
    after
        [ctp(M) || M <- Ms],
        dbg:stop()
    end.

tp({l,M}  ) -> dbg:tpl(M,x);
tp({g,M}  ) -> dbg:tp(M,x);
tp({l,M,F}) -> dbg:tpl(M,F,x);
tp({g,M,F}) -> dbg:tp(M,F,x).

ctp({l,M}  ) -> dbg:ctpl(M);
ctp({g,M}  ) -> dbg:ctp(M);
ctp({l,M,F}) -> dbg:ctpl(M,F);
ctp({g,M,F}) -> dbg:ctp(M,F).
