-module(mnesia_leveled_SUITE).

%% common_test exports
-export(
   [
     all/0, groups/0, suite/0
   , init_per_suite/1, end_per_suite/1
   , init_per_group/2, end_per_group/2
   , init_per_testcase/2, end_per_testcase/2
   ]).


%% test case exports
-export(
   [
     basic_startup/1
   , chg_tbl_copy/1
   , conv_bigtab/1
   , fallback/1
   , indexes/1
   , size_info/1
   ]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [ basic_startup
    , chg_tbl_copy
    , conv_bigtab
    , fallback
    , indexes
    , size_info
    ].

groups() ->
    [].

suite() ->
    [].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.


%% ============================================================
%% Test cases
%% ============================================================


basic_startup(_Config) ->
    ok = mnesia:create_schema([node()]),
    {ok, [leveled, mnesia_leveled]} = application:ensure_all_started(mnesia_leveled),
    ok = mnesia:start(),
    {ok, leveled_copies} = mnesia_leveled:register(),
    [disc_copies, disc_only_copies, leveled_copies, ram_copies] =
        lists:sort(mnesia_schema:backend_types()),
    ok.

chg_tbl_copy(_Config) ->
    mnesia_leveled_chg_tbl_copy:full().

conv_bigtab(_Config) ->
    mnesia_leveled_conv_bigtab:run(10000).

fallback(_Config) ->
    mnesia_leveled_fallback:run().

indexes(_Config) ->
    mnesia_leveled_indexes:run().

size_info(_Config) ->
    mnesia_leveled_size_info:run().
