-module(fdb_subspace_test).

-include_lib("eunit/include/eunit.hrl").

-define(SOLIB,"../priv/fdb_nif").

basic_test()->
  DB = fdb_raw:init_and_open(?SOLIB),
  Space  = fdb_subspace:open(DB,<<"_test">>),
  fdb_subspace:clear_range(Space, nil, nil),
  fdb_subspace:set(Space, "key", "K"),
  SpaceA = fdb_subspace:open(DB,<<"_test_a">>),
  SpaceB = fdb_subspace:open(DB,<<"_test_b">>),
  fdb_subspace:set(SpaceA, "key", "A"),
  fdb_subspace:set(SpaceB, "key", "B"),
  ?assertEqual({ok, "A"}, fdb_subspace:get(SpaceA, "key")),
  ?assertEqual({ok, "B"}, fdb_subspace:get(SpaceB, "key")),
  ?assertEqual( [{"key","B"}]
              , fdb_subspace:get_range(SpaceB, nil, nil)),
  fdb_subspace:clear(SpaceA, "key"),
  fdb_subspace:clear(SpaceB, "key"),
  ?assertEqual(not_found, fdb_subspace:get(SpaceA, "key")),
  ?assertEqual(not_found, fdb_subspace:get(SpaceB, "key")),
  ?assertEqual( []
              , fdb_subspace:get_range(SpaceB, nil, nil)).

