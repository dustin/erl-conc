-module(conc).

%% conc list implementation - a parallel data structure.
%%
%% see:
%% http://research.sun.com/projects/plrg/Publications/ICFPAugust2009Steele.pdf

-export([null/0,
         singleton/1,
         concatenation/2,
         from_list/1, to_list/1,
         first/1, rest/1, append/2,
         mapreduce/4,
         map/2, reduce/3,
         length/1, filter/2, reverse/1,
         foreach/2
        ]).

null() ->
    null.

singleton(X) ->
    {singleton, X}.

concatenation(A, B) ->
    {concatenation, A, B}.

from_list(L) ->
    rebalance(lists:foldl(fun (X, null) ->
                                  singleton(X);
                              (X, Acc) ->
                                  {concatenation, Acc, singleton(X)}
                          end, null, L)).

to_list(C) ->
    lists:flatten(reduce(fun(X,Acc) -> [X] ++ [Acc] end, [], C)).

first(null) ->
    null;
first({singleton, X}) ->
    X;
first({concatenation, A, _B}) ->
    first(A).

rest(null) ->
    null;
rest({singleton, _X}) ->
    null;
rest({concatenation, A, B}) ->
    internal_append(rest(A), B).

append(null, B) ->
    validate(B);
append(A, null) ->
    validate(A);
append(A, B) ->
    rebalance({concatenation, validate(A), validate(B)}).

internal_append(null, B) ->
    B;
internal_append(A, null) ->
    A;
internal_append(A, B) ->
    rebalance({concatenation, A, B}).

validate(null) ->
    null;
validate({singleton, _} = X) -> X;
validate({concatenation, _, _} = X) -> X;
validate(_) -> exit(badarg).

mapreduce(_Map, _Reduce, Id, null) ->
    Id;
mapreduce(Map, _Reduce, _Id, {singleton, I}) ->
    Map(I);
mapreduce(Map, Reduce, Id, {concatenation, A, B}) ->
    %% When both A and B are themselves concatenations, concurrently
    %% mapreduce them.  Otherwise, do normal recursion.
    case {A,B} of
        {{concatenation, _, _}, {concatenation, _, _}} ->
            {VA, VB} = concurrent_mr_helper(Map, Reduce, Id, A, B),
            Reduce(VA, VB);
        _ ->
            Reduce(mapreduce(Map, Reduce, Id, A),
                   mapreduce(Map, Reduce, Id, B))
    end.

map(F, L) ->
    mapreduce(fun (X) -> singleton(F(X)) end,
              fun internal_append/2,
              null,
              L).

reduce(G, Id, L) ->
    mapreduce(fun (X) -> X end,
              G,
              Id,
              L).

length(L) ->
    mapreduce(fun (_X) -> 1 end,
              fun(X,Y) -> X+Y end,
              0,
              L).

filter(F, L) ->
    mapreduce(fun (X) ->
                      case F(X) of
                          true ->
                              singleton(X);
                          _ ->
                              null
                      end
              end,
              fun internal_append/2,
              null,
              L).

reverse(L) ->
    mapreduce(fun singleton/1,
              fun (X, Y) -> internal_append(Y, X) end,
              null,
              L).

foreach(F, L) ->
    mapreduce(fun (X) -> F(X), ok end,
              fun(_X,_Y) -> ok end,
              ok,
              L).

rebalance(null) -> null;
rebalance({singleton, _} = L) -> L;
rebalance({concatenation, A, B} = L) ->
    La = conc:length(A),
    Lb = conc:length(B),
    %% Rebalance this level...
    L2 = if
             La > Lb + 2 -> rebalance(rotate(A, B));
             Lb > La + 2 -> rebalance(rotate(B, A));
             true -> L
         end,
    %% Then recurse to rebalance children.
    case L2 of
        {concatenation, A2, B2} ->
            %% Maybe concurrently rebalance children
            case {A2, B2} of
                {{concatenation, _, _}, {concatenation, _, _}} ->
                    Parent = self(),
                    Pid1 = spawn_link(fun() -> Parent ! {self(), rebalance(A2)} end),
                    Pid2 = spawn_link(fun() -> Parent ! {self(), rebalance(B2)} end),
                    {A3, B3} = {
                      receive {Pid1, X1} -> X1 end,
                      receive {Pid2, X2} -> X2 end},
                    {concatenation, A3, B3};
                _ -> {concatenation, rebalance(A2), rebalance(B2)}
            end;
        _ -> L2
    end.

rotate(null, L) ->
    L;
rotate(L, null) ->
    L;
rotate({singleton, _X} = A, L) ->
    {concatenation, A, L};
rotate({concatenation, A, B}, {singleton, _X} = S) ->
    {concatenation, A, {concatenation, B, S}};
rotate({concatenation, A, B}, {concatenation, _A, _B} = S) ->
    {concatenation, A, {concatenation, B, S}}.

concurrent_mr_helper(Map, Reduce, Id, A, B) ->
    Parent = self(),
    Pid1 = spawn_link(fun() ->
                              Parent ! {self(), mapreduce(Map, Reduce, Id, A)}
                      end),
    Pid2 = spawn_link(fun() ->
                              Parent ! {self(), mapreduce(Map, Reduce, Id, B)}
                      end),
    {receive {Pid1, X1} -> X1 end, receive {Pid2, X2} -> X2 end}.
