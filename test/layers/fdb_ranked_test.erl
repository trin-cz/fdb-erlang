-module(fdb_ranked_test).

-include_lib("eunit/include/eunit.hrl").
-include("../include/fdb.hrl").

-define(SOLIB,"../priv/fdb_nif").

basic_test()->
  {ok, DB} = fdb_raw:init_and_open_try_5_times(?SOLIB),
  Ranked = fdb_ranked:open(DB,<<"__test">>),
  [ok = fdb_ranked:set(Ranked, I, I) || I <- lists:seq(1, 9)],
  ?assertEqual( [{2, 2}, {3, 3},{4, 4}]
              , fdb_ranked:get_range(Ranked, #select{gte = 2, lte = 4})),
  ?assertEqual( [  {ok, I}
                || I <- lists:seq(1, 9)
                ]
              , [  fdb_ranked:get_rank(Ranked, I)
                || I <- lists:seq(1, 9)
                ]),
  ?assertEqual( 9
              , fdb_ranked:get_size(Ranked)),
  Subspace = fdb_subspace:open(DB,<<"__test">>),
  fdb_subspace:clear_range(Subspace, nil, nil),
  ok.

