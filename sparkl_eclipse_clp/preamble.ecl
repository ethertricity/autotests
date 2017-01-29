%%% @copyright 2016 SPARKL Limited. All Rights Reserved.
%%% @author <ahfarrell@sparkl.com> Andrew Farrell
%%% @version {@version}
%%% @doc
%%% Predefined theory for SPARKL Eclipse-CLP integration.
%%%
:- dynamic ecl__field_out/2.
:- dynamic ecl__resultname/1.
:- dynamic ecl__result/1.
:- dynamic ecl__field_in/2.
:- lib(ic).
:- lib(sd).

do__it(ResultFile, Goal) :-
    call(Goal),
    open(ResultFile, write, Stream),
    findall((X, Y), clause(ecl__field_out(X,Y)), FieldList),
    write(Stream, "{"),
    ( clause(ecl__result(Result)) ->
      write(Stream, "\"result\": "),
      write(Stream, Result),
      write(Stream, ", ");
      true
    ),

    ( clause(ecl__resultname(ResName)) ->
      write(Stream, "\"outputname\": \""),
      write(Stream, ResName),
      write(Stream, "\", ");
      true
    ),
    write(Stream, "\"fields\": "),
    write(Stream, "{"),
    write__results(Stream, FieldList, first),
    write(Stream, "}"),
    write(Stream, "}"),
    close(Stream).

write__results(_Stream, [], _First) :- !.

write__results(Stream, [(ResKey, ResVal) | RemRes], notfirst) :-
    write(Stream, ", "), 
    write__results(Stream, [(ResKey, ResVal) | RemRes], first).

write__results(Stream, [(ResKey, ResVal) | RemRes], first) :-
    write__key(Stream, ResKey),
    write(Stream, ":"),
    write__val(Stream, ResVal),
    write__results(Stream, RemRes, notfirst).

write__key(Stream, Key) :-
    write(Stream, "\""),
    write(Stream, Key),
    write(Stream, "\"").

write__val(Stream, Val) :-
    ( string(Val) ->
      write(Stream, "\""),
      write(Stream, Val),
      write(Stream, "\"");
      write(Stream, Val)
    ).

resultfield([], []):- !.

resultfield([K|Ks], [V|Vs]):- !,
    resultfield(K, V),
    resultfield(Ks, Vs).

resultfield(Key, Val):-
    assert(ecl__field_out(Key, Val)).

resultname(ResName) :-
    assert(ecl__resultname(ResName)).

result(Result) :-
    assert(ecl__result(Result)).


field([], []) :- !.

field([K|Ks], [V|Vs]) :- !,
  field(K, V),
  field(Ks, Vs).

field(K, V) :-
  ecl__field_in(K, V).


before__(X, Y, Z) :-
    X #= Y - Z.

after__(X, Y, Z) :-
    X #= Y + Z.

time__([H_], T) :-
    !,
    number_string(H, H_),
    T is H * 3600.
time__([H_, M_], T) :-
    !,
    number_string(H, H_),
    number_string(M, M_),
    T is H * 3600 + M * 60.
time__([H_, M_, S_], T) :-
    !,
    number_string(H, H_),
    number_string(M, M_),
    number_string(S, S_),
    T is H * 3600 + M * 60 + S.

time([], []) :- !.

time([H_|T_], [H|T]) :- !,
  time(H_, H),
  time(T_, T).

time(T__, T) :-
    split_string(T__, ":", "", T_),
    time__(T_, T).


timestr([], []) :- !.

timestr([H_|T_], [H|T]) :- !,
  timestr(H_, H),
  timestr(T_, T).

timestr(MT_, T) :-
    MT_ < 0, !,
    T_ is -MT_,
    H_ is T_ // 3600,
    D__ is (H_ // 24),
    D___ is D__ + 1,
    H__ is H_ - (D__ * 24),
    H___ is (24 - H__),
    T__ is T_ - (H_ * 3600),

    M_ is T__ // 60,
    T___ is T__ - (M_ * 60),
    M___ is (60 - M_),
    S___ is (60 - T___),

    number_string(D___, D),
    number_string(H___, H),
    number_string(M___, M),
    number_string(S___, S),

    concat_string([H,":",M,":",S,"-",D], T).


timestr(T_, T) :-
    H_ is T_ // 3600,
    D__ is H_ // 24,
    H__ is H_ - (D__ * 24),
    T__ is T_ - (H_ * 3600),
    M__ is T__ // 60,
    S__ is T__ - (M__ * 60),
    number_string(D__, D),
    number_string(H__, H),
    number_string(M__, M),
    number_string(S__, S),
    concat_string([H,":",M,":",S,"+",D], T).


before(X, Y, T_) :-
    time(T_, T),
    before__(X, Y, T).

after(X, Y, T_) :-
    time(T_, T),
    after__(X, Y, T).

