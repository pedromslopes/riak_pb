%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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
%%
%% -------------------------------------------------------------------
-module(antidote_pb_codec).

-include("riak_pb.hrl").
-include("antidote_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([encode/2,
  decode/2,
  decode_response/1,
  encode_read_objects/2,
  decode_bound_object/1,
  encode_update_objects/2,
  decode_update_op/1]).

-define(TYPE_COUNTER, counter).
-define(TYPE_SET, set).


-define(assert_binary(X), case is_binary(X) of true -> ok; false -> throw({not_binary, X}) end).
-define(assert_all_binary(Xs), [?assert_binary(X) || X <- Xs]).
-define(assert_all_binary2(Xss), lists:foldl(fun(Elem,AccIn) -> case Elem of ok -> AccIn; _ -> false end end,true,?assert_all_binary(Xss))).

% general encode function
encode(start_transaction, {Clock, Properties}) ->
  encode_start_transaction(Clock, Properties);
encode(txn_properties, Props) ->
  encode_txn_properties(Props);
encode(abort_transaction, TxId) ->
  encode_abort_transaction(TxId);
encode(commit_transaction, TxId) ->
  encode_commit_transaction(TxId);
encode(update_objects, {Updates, TxId}) ->
  encode_update_objects(Updates, TxId);
encode(update_op, {Object, Op, Param}) ->
  encode_update_op(Object, Op, Param);
encode(static_update_objects, {Clock, Properties, Updates}) ->
  encode_static_update_objects(Clock, Properties, Updates);
encode(bound_object, {Key, Type, Bucket}) ->
  encode_bound_object(Key, Type, Bucket);
encode(type, Type) ->
  encode_type(Type);
encode(reg_update, Update) ->
  encode_reg_update(Update);
encode(counter_update, Update) ->
  encode_counter_update(Update);
encode(set_update, Update) ->
  encode_set_update(Update);
encode(read_objects, {Objects, TxId}) ->
  encode_read_objects(Objects, TxId);
encode(static_read_objects, {Clock, Properties, Objects}) ->
  encode_static_read_objects(Clock, Properties, Objects);
encode(query_objects, {Filter, TxId}) ->
  encode_query_objects(Filter, TxId);
encode(start_transaction_response, Resp) ->
  encode_start_transaction_response(Resp);
encode(operation_response, Resp) ->
  encode_operation_response(Resp);
encode(commit_response, Resp) ->
  encode_commit_response(Resp);
encode(read_objects_response, Resp) ->
  encode_read_objects_response(Resp);
encode(read_object_resp, Resp) ->
  encode_read_object_resp(Resp);
encode(static_read_objects_response, {ok, Results, CommitTime}) ->
  encode_static_read_objects_response(Results, CommitTime);
encode(query_objects_response, Resp) ->
  encode_query_objects_response(Resp);
encode(error_code, Code) ->
  encode_error_code(Code);
encode(_Other, _) ->
  erlang:error("Incorrect operation/Not yet implemented").

% general decode function
decode(txn_properties, Properties) ->
  decode_txn_properties(Properties);
decode(bound_object, Obj) ->
  decode_bound_object(Obj);
decode(type, Type) ->
  decode_type(Type);
decode(error_code, Code) ->
  decode_error_code(Code);
decode(update_object, Obj) ->
  decode_update_op(Obj);
decode(reg_update, Update) ->
  decode_reg_update(Update);
decode(counter_update, Update) ->
  decode_counter_update(Update);
decode(set_update, Update) ->
  decode_set_update(Update);
decode(_Other, _) ->
  erlang:error("Unknown message").


%%%%%%%%%%%%%%%%%%%%%%%%%%%
% error codes

encode_error_code(unknown) -> 0;
encode_error_code(timeout) -> 1;
encode_error_code(_Other) -> 0.

decode_error_code(0) -> unknown;
decode_error_code(1) -> timeout.


%%%%%%%%%%%%%%%%%%%%%%%
% Transactions

encode_start_transaction(Clock, Properties) ->
  case Clock of
    ignore ->
      #apbstarttransaction{
        properties = encode_txn_properties(Properties)};
    _ ->
      #apbstarttransaction{timestamp = Clock,
        properties = encode_txn_properties(Properties)}
  end.


encode_commit_transaction(TxId) ->
  #apbcommittransaction{transaction_descriptor = TxId}.

encode_abort_transaction(TxId) ->
  #apbaborttransaction{transaction_descriptor = TxId}.


encode_txn_properties(Props) ->
  % 0 = not_specified | 1 = use_default | 2 = certify | 3 = dont_certify
  Certify_Value = case orddict:find(certify,Props) of
        error -> 0;
        {ok, use_default} -> 1;
        {ok, certify} -> 2;
        {ok, dont_certify} -> 3;
        _ -> 0
  end,
  Update_Clock_Value = case orddict:find(update_clock,Props) of
        error -> 0;
        {ok, true} -> 1;
        {ok, false} -> 2;
        _ -> 0
  end,
  Locks_Value = case orddict:find(locks,Props) of
        error -> [];
        {ok, Value1} -> 
            case ?assert_all_binary2(Value1) of
                true ->
                    Value1;
                false ->
                    [term_to_binary(A)||A<-Value1]
            end;
        _ -> []
  end,
  Shared_Locks_Value = case orddict:find(shared_locks,Props) of
        error -> [];
        {ok, Value2} -> 
            case ?assert_all_binary2(Value2) of
                true ->
                    Value2;
                false ->
                    [term_to_binary(A)||A<-Value2]
            end;
        _ -> []
  end,
  Exclusive_Locks_Value = case orddict:find(exclusive_locks,Props) of
        error -> [];
        {ok, Value3} -> 
            case ?assert_all_binary2(Value3) of
                true ->
                    Value3;
                false ->
                    [term_to_binary(A)||A<-Value3]
            end;
        _ -> []
  end,
  #apbtxnproperties{certify = Certify_Value,
    locks = Locks_Value,
    shared_locks = Shared_Locks_Value,
    exclusive_locks = Exclusive_Locks_Value,
    update_clock = Update_Clock_Value}.

decode_txn_properties(Properties) ->
  #apbtxnproperties{certify = Certify_Value,
    locks = Locks_Value,
    shared_locks = Shared_Locks_Value,
    exclusive_locks = Exclusive_Locks_Value,
    update_clock = Update_Clock_Value} = Properties,
  Properties_List_0 = orddict:new(),
  % 0 = not_specified | 1 = use_default | 2 = certify | 3 = dont_certify
  Properties_List_1 = case Certify_Value of
      0 -> Properties_List_0;
      1 -> orddict:store(certify,use_default,Properties_List_0);
      2 -> orddict:store(certify,certify,Properties_List_0);
      3 -> orddict:store(certify,dont_certify,Properties_List_0);
      _ -> Properties_List_0
  end,
  Properties_List_2 = case Locks_Value of
      [] -> Properties_List_1;
      % Lock_List -> orddict:store(locks,[binary_to_term(Lock)||Lock<-Lock_List],Properties_List_1)
      _Lock_List -> orddict:store(locks,Locks_Value, Properties_List_1)
  end,
  Properties_List_3 = case Shared_Locks_Value of
      [] -> Properties_List_2;
      _Shared_Lock_List -> orddict:store(shared_locks,Shared_Locks_Value,Properties_List_2)
  end,
  Properties_List_4 = case Exclusive_Locks_Value of
      [] -> Properties_List_3;
      _Exclusive_Lock_List -> orddict:store(exclusive_locks,Exclusive_Locks_Value,Properties_List_3)
  end,
  _Properties_List_5 = case Update_Clock_Value of
      0 -> Properties_List_4;
      1 -> orddict:store(update_clock,true,Properties_List_4);
      2 -> orddict:store(update_clock,false,Properties_List_4);
      _ -> Properties_List_4
  end.

%%%%%%%%%%%%%%%%%%%%%
%% Updates

% bound objects

encode_bound_object({Key, Type, Bucket}) ->
  encode_bound_object(Key, Type, Bucket).
encode_bound_object(Key, Type, Bucket) ->
  #apbboundobject{key = Key, type = encode_type(Type), bucket = Bucket}.

decode_bound_object(Obj) ->
  #apbboundobject{key = Key, type = Type, bucket = Bucket} = Obj,
  {Key, decode_type(Type), Bucket}.


% static_update_objects

encode_static_update_objects(Clock, Properties, Updates) ->
  EncTransaction = encode_start_transaction(Clock, Properties),
  EncUpdates = lists:map(fun(Update) ->
    encode_update_op(Update) end,
    Updates),
  #apbstaticupdateobjects{transaction = EncTransaction,
    updates = EncUpdates}.

decode_update_op(Obj) ->
  #apbupdateop{boundobject = Object, operation = Operation} = Obj,
  {Op, OpParam} = decode_update_operation(Operation),
  {decode_bound_object(Object), Op, OpParam}.

encode_update_op({Object, Op, Param}) ->
  encode_update_op(Object, Op, Param).
encode_update_op(Object, Op, Param) ->
  {_Key, Type, _Bucket} = Object,
  EncObject = encode_bound_object(Object),
  Operation = encode_update_operation(Type, {Op, Param}),
  #apbupdateop{boundobject = EncObject, operation = Operation}.

encode_update_objects(Updates, TxId) ->
  EncUpdates = lists:map(fun(Update) ->
    encode_update_op(Update) end,
    Updates),
  #apbupdateobjects{updates = EncUpdates, transaction_descriptor = TxId}.


%%%%%%%%%%%%%%%%%%%%%%%%
%% Responses

encode_static_read_objects_response(Results, CommitTime) ->
  #apbstaticreadobjectsresp{
    objects = encode_read_objects_response({ok, Results}),
    committime = encode_commit_response({ok, CommitTime})}.


encode_read_objects_response({error, Reason}) ->
  #apbreadobjectsresp{success = false, errorcode = encode_error_code(Reason)};
encode_read_objects_response({ok, Results}) ->
  EncResults = lists:map(fun(R) ->
    encode_read_object_resp(R) end,
    Results),
  #apbreadobjectsresp{success = true, objects = EncResults}.

encode_query_objects_response({error, Reason}) ->
  #apbqueryobjectsresp{success = false, errorcode = encode_error_code(Reason)};
encode_query_objects_response({ok, Results}) ->
  #apbqueryobjectsresp{success = true, result = Results}.

encode_start_transaction_response({error, Reason}) ->
  #apbstarttransactionresp{success = false, errorcode = encode_error_code(Reason)};
encode_start_transaction_response({ok, TxId}) ->
  #apbstarttransactionresp{success = true, transaction_descriptor = term_to_binary(TxId)}.

encode_operation_response({error, Reason}) ->
  #apboperationresp{success = false, errorcode = encode_error_code(Reason)};
encode_operation_response(ok) ->
  #apboperationresp{success = true}.

encode_commit_response({error, Reason}) ->
  #apbcommitresp{success = false, errorcode = encode_error_code(Reason)};

encode_commit_response({ok, CommitTime}) ->
  #apbcommitresp{success = true, commit_time = term_to_binary(CommitTime)}.

decode_response(#apboperationresp{success = true}) ->
  {opresponse, ok};
decode_response(#apboperationresp{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#apbstarttransactionresp{success = true,
  transaction_descriptor = TxId}) ->
  {start_transaction, TxId};
decode_response(#apbstarttransactionresp{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#apbcommitresp{success = true, commit_time = TimeStamp}) ->
  {commit_transaction, TimeStamp};
decode_response(#apbcommitresp{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#apbreadobjectsresp{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#apbreadobjectsresp{success = true, objects = Objects}) ->
  Resps = lists:map(fun(O) ->
    decode_response(O) end,
    Objects),
  {read_objects, Resps};
decode_response(#apbreadobjectresp{} = ReadObjectResp) ->
  decode_read_object_resp(ReadObjectResp);
decode_response(#apbstaticreadobjectsresp{objects = Objects,
  committime = CommitTime}) ->
  {read_objects, Values} = decode_response(Objects),
  {commit_transaction, TimeStamp} = decode_response(CommitTime),
  {static_read_objects_resp, Values, TimeStamp};
decode_response(#apbqueryobjectsresp{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#apbqueryobjectsresp{success = true, result = Result}) ->
  {query_objects, Result};
decode_response(Other) ->
  erlang:error("Unexpected message: ~p", [Other]).

%%%%%%%%%%%%%%%%%%%%%%
%% Reading objects


encode_static_read_objects(Clock, Properties, Objects) ->
  EncTransaction = encode_start_transaction(Clock, Properties),
  EncObjects = lists:map(fun(Object) ->
    encode_bound_object(Object) end,
    Objects),
  #apbstaticreadobjects{transaction = EncTransaction,
    objects = EncObjects}.

encode_read_objects(Objects, TxId) ->
  BoundObjects = lists:map(fun(Object) ->
    encode_bound_object(Object) end,
    Objects),
  #apbreadobjects{boundobjects = BoundObjects, transaction_descriptor = TxId}.

%%%%%%%%%%%%%%%%%%%%%%
%% Querying objects

encode_query_objects(Filter, TxId) ->
  #apbqueryobjects{filter = Filter, transaction_descriptor = TxId}.

%%%%%%%%%%%%%%%%%%%
%% Crdt types

%%COUNTER = 3;
%%ORSET = 4;
%%LWWREG = 5;
%%MVREG = 6;
%%GMAP = 8;
%%AWMAP = 9;
%%RWSET = 10;

encode_type(antidote_crdt_counter_pn) -> 'COUNTER';
encode_type(antidote_crdt_counter_fat) -> 'FATCOUNTER';
encode_type(antidote_crdt_counter_b) -> 'BCOUNTER';
encode_type(antidote_crdt_set_aw) -> 'ORSET';
encode_type(antidote_crdt_register_lww) -> 'LWWREG';
encode_type(antidote_crdt_register_mv) -> 'MVREG';
encode_type(antidote_crdt_map_go) -> 'GMAP';
encode_type(antidote_crdt_set_rw) -> 'RWSET';
encode_type(antidote_crdt_map_rr) -> 'RRMAP';
encode_type(antidote_crdt_flag_ew) -> 'FLAG_EW';
encode_type(antidote_crdt_flag_dw) -> 'FLAG_DW';
encode_type(antidote_crdt_index_p) -> 'INDEX_P';
encode_type(antidote_crdt_index) -> 'INDEX';
encode_type(T) -> erlang:error({unknown_crdt_type, T}).


decode_type('COUNTER') -> antidote_crdt_counter_pn;
decode_type('FATCOUNTER') -> antidote_crdt_counter_fat;
decode_type('BCOUNTER') -> antidote_crdt_counter_b;
decode_type('ORSET') -> antidote_crdt_set_aw;
decode_type('LWWREG') -> antidote_crdt_register_lww;
decode_type('MVREG') -> antidote_crdt_register_mv;
decode_type('GMAP') -> antidote_crdt_map_go;
decode_type('RWSET') -> antidote_crdt_set_rw;
decode_type('RRMAP') -> antidote_crdt_map_rr;
decode_type('FLAG_EW') -> antidote_crdt_flag_ew;
decode_type('FLAG_DW') -> antidote_crdt_flag_dw;
decode_type('INDEX_P') -> antidote_crdt_index_p;
decode_type('INDEX') -> antidote_crdt_index;
decode_type(T) -> erlang:error({unknown_crdt_type_protobuf, T}).


%%%%%%%%%%%%%%%%%%%%%%
% CRDT operations


encode_update_operation(_Type, {reset, {}}) ->
  #apbupdateoperation{resetop = #apbcrdtreset{}};
encode_update_operation(antidote_crdt_counter_pn, Op_Param) ->
  #apbupdateoperation{counterop = encode_counter_update(Op_Param)};
encode_update_operation(antidote_crdt_counter_fat, Op_Param) ->
  #apbupdateoperation{counterop = encode_counter_update(Op_Param)};
encode_update_operation(antidote_crdt_counter_b, Op_Param) ->
  #apbupdateoperation{bcounterop = encode_counter_update(Op_Param)};
encode_update_operation(antidote_crdt_set_aw, Op_Param) ->
  #apbupdateoperation{setop = encode_set_update(Op_Param)};
encode_update_operation(antidote_crdt_set_rw, Op_Param) ->
  #apbupdateoperation{setop = encode_set_update(Op_Param)};
encode_update_operation(antidote_crdt_register_lww, Op_Param) ->
  #apbupdateoperation{regop = encode_reg_update(Op_Param)};
encode_update_operation(antidote_crdt_register_mv, Op_Param) ->
  #apbupdateoperation{regop = encode_reg_update(Op_Param)};
encode_update_operation(antidote_crdt_map_go, Op_Param) ->
  #apbupdateoperation{mapop = encode_map_update(Op_Param)};
encode_update_operation(antidote_crdt_map_rr, Op_Param) ->
  #apbupdateoperation{mapop = encode_map_update(Op_Param)};
encode_update_operation(antidote_crdt_flag_ew, Op_Param) ->
  #apbupdateoperation{flagop = encode_flag_update(Op_Param)};
encode_update_operation(antidote_crdt_flag_dw, Op_Param) ->
  #apbupdateoperation{flagop = encode_flag_update(Op_Param)};
encode_update_operation(antidote_crdt_index_p, Op_Param) ->
  #apbupdateoperation{indexpop = encode_indexp_update(Op_Param)};
encode_update_operation(antidote_crdt_index, Op_Param) ->
  #apbupdateoperation{indexsop = encode_index_s_update(Op_Param)};
encode_update_operation(Type, _Op) ->
  throw({invalid_type, Type}).

decode_update_operation(#apbupdateoperation{counterop = Op}) when Op /= undefined ->
  decode_counter_update(Op);
decode_update_operation(#apbupdateoperation{bcounterop = Op}) when Op /= undefined ->
  decode_counter_update(Op);
decode_update_operation(#apbupdateoperation{setop = Op}) when Op /= undefined ->
  decode_set_update(Op);
decode_update_operation(#apbupdateoperation{regop = Op}) when Op /= undefined ->
  decode_reg_update(Op);
decode_update_operation(#apbupdateoperation{mapop = Op}) when Op /= undefined ->
  decode_map_update(Op);
decode_update_operation(#apbupdateoperation{flagop = Op}) when Op /= undefined ->
  decode_flag_update(Op);
decode_update_operation(#apbupdateoperation{indexpop = Op}) when Op /= undefined ->
  decode_index_p_update(Op);
decode_update_operation(#apbupdateoperation{indexsop = Op}) when Op /= undefined ->
  decode_index_s_update(Op);
decode_update_operation(#apbupdateoperation{resetop = #apbcrdtreset{}}) ->
  {reset, {}}.


%%decode_update_operation(#apbupdateoperation{mapop = Op}) when Op /= undefined ->
%%  decode_map_update(Op);
%%decode_update_operation(#apbupdateoperation{resetop = Op}) when Op /= undefined ->
%%  decode_reset_update(Op).

% general encoding of CRDT responses

encode_read_object_resp({{_Key, Type, _Bucket}, Val}) ->
  encode_read_object_resp(Type, Val).

encode_read_object_resp(antidote_crdt_register_lww, Val) ->
  #apbreadobjectresp{reg = #apbgetregresp{value = Val}};
encode_read_object_resp(antidote_crdt_register_mv, Vals) ->
  #apbreadobjectresp{mvreg = #apbgetmvregresp{values = Vals}};
encode_read_object_resp(antidote_crdt_counter_pn, Val) ->
  #apbreadobjectresp{counter = #apbgetcounterresp{value = Val}};
encode_read_object_resp(antidote_crdt_counter_fat, Val) ->
  #apbreadobjectresp{counter = #apbgetcounterresp{value = Val}};
encode_read_object_resp(antidote_crdt_counter_b, Val) ->
  #apbreadobjectresp{counter_b = encode_bcounter_resp(Val)};
encode_read_object_resp(antidote_crdt_set_aw, Val) ->
  #apbreadobjectresp{set = #apbgetsetresp{value = Val}};
encode_read_object_resp(antidote_crdt_set_rw, Val) ->
  #apbreadobjectresp{set = #apbgetsetresp{value = Val}};
encode_read_object_resp(antidote_crdt_map_go, Val) ->
  #apbreadobjectresp{map = encode_map_get_resp(Val)};
encode_read_object_resp(antidote_crdt_map_rr, Val) ->
  #apbreadobjectresp{map = encode_map_get_resp(Val)};
encode_read_object_resp(antidote_crdt_flag_ew, Val) ->
  #apbreadobjectresp{flag = #apbgetflagresp{value = Val}};
encode_read_object_resp(antidote_crdt_flag_dw, Val) ->
  #apbreadobjectresp{flag = #apbgetflagresp{value = Val}};
encode_read_object_resp(antidote_crdt_index_p, Val) ->
  #apbreadobjectresp{index = encode_index_get_resp(Val)};
encode_read_object_resp(antidote_crdt_index, Val) ->
  #apbreadobjectresp{index = encode_index_get_resp(Val)}.

% TODO why does this use counter instead of antidote_crdt_counter_pn etc.?
decode_read_object_resp(#apbreadobjectresp{counter = #apbgetcounterresp{value = Val}}) ->
  {counter, Val};
decode_read_object_resp(#apbreadobjectresp{counter_b = CounterResp = #apbgetbcounterresp{}}) ->
  {counter_b, decode_bcounter_resp(CounterResp)};
decode_read_object_resp(#apbreadobjectresp{set = #apbgetsetresp{value = Val}}) ->
  {set, Val};
decode_read_object_resp(#apbreadobjectresp{reg = #apbgetregresp{value = Val}}) ->
  {reg, Val};
decode_read_object_resp(#apbreadobjectresp{mvreg = #apbgetmvregresp{values = Vals}}) ->
  {mvreg, Vals};
decode_read_object_resp(#apbreadobjectresp{map = MapResp = #apbgetmapresp{}}) ->
  {map, decode_map_get_resp(MapResp)};
decode_read_object_resp(#apbreadobjectresp{flag = #apbgetflagresp{value = Val}}) ->
  {flag, Val};
decode_read_object_resp(#apbreadobjectresp{index = IndexResp = #apbgetindexresp{}}) ->
  {index, decode_index_get_resp(IndexResp)}.

% set updates

encode_set_update({add, Elem}) ->
  ?assert_binary(Elem),
  #apbsetupdate{optype = 'ADD', adds = [Elem]};
encode_set_update({add_all, Elems}) ->
  ?assert_all_binary(Elems),
  #apbsetupdate{optype = 'ADD', adds = Elems};
encode_set_update({remove, Elem}) ->
  ?assert_binary(Elem),
  #apbsetupdate{optype = 'REMOVE', rems = [Elem]};
encode_set_update({remove_all, Elems}) ->
  ?assert_all_binary(Elems),
  #apbsetupdate{optype = 'REMOVE', rems = Elems}.

decode_set_update(Update) ->
  #apbsetupdate{optype = OpType, adds = A, rems = R} = Update,
  case OpType of
    'ADD' ->
      case A of
        undefined -> [];
        [Elem] -> {add, Elem};
        AddElems when is_list(AddElems) -> {add_all, AddElems}
      end;
    'REMOVE' ->
      case R of
        undefined -> [];
        [Elem] -> {remove, Elem};
        Elems when is_list(Elems) -> {remove_all, Elems}
      end
  end.

% counter updates

encode_counter_update({increment, {Amount, Actor}}) ->
  #apbbcounterupdate{inc = Amount, actor = Actor};
encode_counter_update({increment, Amount}) ->
  #apbcounterupdate{inc = Amount};
encode_counter_update({decrement, {Amount, Actor}}) ->
  #apbbcounterupdate{inc = -Amount, actor = Actor};
encode_counter_update({decrement, Amount}) ->
  #apbcounterupdate{inc = -Amount};
encode_counter_update({transfer, {Amount, To, Actor}}) ->
  #apbbcounterupdate{inc = Amount, transferdest = To, actor = Actor}.

decode_counter_update(#apbcounterupdate{inc = I}) ->
  case I of
    undefined -> {increment, 1};
    I -> {increment, I} % negative value for I indicates decrement
  end;
decode_counter_update(#apbbcounterupdate{inc = I, actor = Actor, transferdest = undefined}) ->
  case I of
    undefined -> {increment, {1, Actor}};
    I when I > 0 -> {increment, {I, Actor}};
    I when I < 0 -> {decrement, {-I, Actor}}
  end;
decode_counter_update(#apbbcounterupdate{inc = Amount, actor = Actor, transferdest = To}) ->
  {transfer, {Amount, To, Actor}}.

% bounded counter responses
encode_bcounter_resp({Incs, Decs}) ->
  EncInc = [encode_bcounter_entry(Inc) || Inc <- Incs],
  EncDec = [encode_bcounter_entry(Dec) || Dec <- Decs],
  #apbgetbcounterresp{incs = EncInc, decs = EncDec}.

encode_bcounter_entry({Actor, Value}) ->
  #apbbcounterentry{actor = term_to_binary(Actor), value = Value}.

decode_bcounter_resp(#apbgetbcounterresp{incs = EncIncs, decs = EncDecs}) ->
  DecIncs = [decode_bcounter_entry(EncInc) || EncInc <- EncIncs],
  DecDecs = [decode_bcounter_entry(EncDec) || EncDec <- EncDecs],
  {DecIncs, DecDecs}.

decode_bcounter_entry(#apbbcounterentry{actor = Actor, value = Value}) ->
  {binary_to_term(Actor), Value}.

% register updates

encode_reg_update(Update) ->
  {assign, Value} = Update,
  #apbregupdate{value = Value}.


decode_reg_update(Update) ->
  #apbregupdate{value = Value} = Update,
  {assign, Value}.

% flag updates

encode_flag_update({enable, {}}) ->
  #apbflagupdate{value = true};
encode_flag_update({disable, {}}) ->
  #apbflagupdate{value = false}.

decode_flag_update(#apbflagupdate{value = true}) ->
  {enable, {}};
decode_flag_update(#apbflagupdate{value = false}) ->
  {disable, {}}.

% map updates

encode_map_update({update, Ops}) when is_list(Ops) ->
  encode_map_update({batch, {Ops, []}});
encode_map_update({update, Op}) ->
  encode_map_update({batch, {[Op], []}});
encode_map_update({remove, Keys}) when is_list(Keys) ->
  encode_map_update({batch, {[], Keys}});
encode_map_update({remove, Key}) ->
  encode_map_update({batch, {[], [Key]}});
encode_map_update({batch, {Updates, RemovedKeys}}) ->
  UpdatesEnc = [encode_map_nested_update(U) || U <- Updates],
  RemovedKeysEnc = [encode_map_key(K) || K <- RemovedKeys],
  #apbmapupdate{updates = UpdatesEnc, removedkeys = RemovedKeysEnc}.


decode_map_update(#apbmapupdate{updates = [Update], removedkeys = []}) ->
  {update, decode_map_nested_update(Update)};
decode_map_update(#apbmapupdate{updates = Updates, removedkeys = []}) ->
  {update, [decode_map_nested_update(U) || U <- Updates]};
decode_map_update(#apbmapupdate{updates = [], removedkeys = [Key]}) ->
  {remove, decode_map_key(Key)};
decode_map_update(#apbmapupdate{updates = [], removedkeys = Keys}) ->
  {remove, [decode_map_key(K) || K <- Keys]};
decode_map_update(#apbmapupdate{updates = Updates, removedkeys = Keys}) ->
  {batch, {[decode_map_nested_update(U) || U <- Updates], [decode_map_key(K) || K <- Keys]}}.


encode_map_nested_update({{Key, Type}, Update}) ->
  #apbmapnestedupdate{
    key = encode_map_key({Key, Type}),
    update = encode_update_operation(Type, Update)
  }.

decode_map_nested_update(#apbmapnestedupdate{key = KeyEnc, update = UpdateEnc}) ->
  {Key, Type} = decode_map_key(KeyEnc),
  Update = decode_update_operation(UpdateEnc),
  {{Key, Type}, Update}.

encode_map_key({Key, Type}) ->
  ?assert_binary(Key),
  #apbmapkey{
    key = Key,
    type = encode_type(Type)
  }.

decode_map_key(#apbmapkey{key = Key, type = Type}) ->
  {Key, decode_type(Type)}.

% map responses

encode_map_get_resp(Entries) ->
  #apbgetmapresp{entries = [encode_map_entry(E) || E <- Entries]}.

decode_map_get_resp(#apbgetmapresp{entries = Entries}) ->
  [decode_map_entry(E) || E <- Entries].

encode_map_entry({{Key, Type}, Val}) ->
  #apbmapentry{
    key = encode_map_key({Key, Type}),
    value = encode_read_object_resp(Type, Val)
  }.

decode_map_entry(#apbmapentry{key = KeyEnc, value = ValueEnc}) ->
  {Key, Type} = decode_map_key(KeyEnc),
  {_Tag, Value} = decode_read_object_resp(ValueEnc),
  {{Key, Type}, Value}.


% index updates
encode_index_key(Key) when is_binary(Key) ->
  #apbindexkey{bytekey = Key};
encode_index_key(Key) when is_integer(Key) ->
  #apbindexkey{intkey = Key};
encode_index_key(Key) when is_boolean(Key) ->
  #apbindexkey{boolkey = Key};
encode_index_key(Key) ->
  #apbindexkey{bytekey = term_to_binary(Key)}.

decode_index_key(#apbindexkey{
  bytekey = Key,
  intkey = undefined,
  boolkey = undefined
}) ->
  Key;
decode_index_key(#apbindexkey{
  bytekey = undefined,
  intkey = Key,
  boolkey = undefined
}) ->
  Key;
decode_index_key(#apbindexkey{
  bytekey = undefined,
  intkey = undefined,
  boolkey = Key
}) ->
  Key.

%% index_p
encode_indexp_update({update, Ops}) when is_list(Ops) ->
  UpdatesEnc = lists:map(fun(Op) -> encode_indexp_nested_update(Op) end, Ops),
  #apbindexpupdate{updates = UpdatesEnc};
encode_indexp_update({update, {Key, Op}}) ->
  encode_indexp_update({update, [{Key, Op}]});
encode_indexp_update({remove, Keys}) when is_list(Keys) ->
  RemovesEnc = Keys,
  #apbindexpupdate{removes = RemovesEnc};
encode_indexp_update({remove, Key}) ->
  #apbindexpupdate{removes = [Key]}.

encode_indexp_nested_update({Key, Op}) ->
  #apbindexpnestedupdate{
    key = encode_index_key(Key),
    update = encode_reg_update(Op)
  }.

decode_index_p_update(#apbindexpupdate{updates = [Update], removes = []}) ->
  {update, decode_index_p_nested_update(Update)};
decode_index_p_update(#apbindexpupdate{updates = Updates, removes = []}) ->
  DecUpdates = lists:map(fun(EncUpdate) -> decode_index_p_nested_update(EncUpdate) end, Updates),
  {update, DecUpdates};
decode_index_p_update(#apbindexpupdate{updates = [], removes = [Key]}) ->
  {remove, decode_index_key(Key)};
decode_index_p_update(#apbindexpupdate{updates = [], removes = Keys}) ->
  DecKeys = lists:map(fun(EncKey) -> decode_index_key(EncKey) end, Keys),
  {remove, DecKeys}.

decode_index_p_nested_update(#apbindexpnestedupdate{key = KeyEnc, update = UpdateEnc}) ->
  Key = decode_index_key(KeyEnc),
  Update = decode_reg_update(UpdateEnc),
  {Key, Update}.

% index_s
encode_index_s_update({update, Ops}) when is_list(Ops) ->
  UpdatesEnc =
    lists:map(fun({Key, Ops1}) -> encode_index_s_single_update({Key, Ops1}) end, Ops),
  #apbindexsupdate{updates = UpdatesEnc};
encode_index_s_update({update, {Key, Ops}}) ->
  #apbindexsupdate{updates = [encode_index_s_single_update({Key, Ops})]};
encode_index_s_update({remove, Keys}) when is_list(Keys) ->
  RemovesEnc = Keys,
  #apbindexsupdate{removes = RemovesEnc};
encode_index_s_update({remove, Key}) ->
  #apbindexsupdate{removes = [Key]}.

encode_index_s_single_update({Key, Ops}) when is_list(Ops) ->
  EncSubOps =
    lists:map(fun({FieldName, FieldType, Op}) ->
      ?assert_binary(FieldName),
      #apbindexstupleoperation{
        fieldname = encode_field(FieldName),
        fieldtype = encode_type(FieldType),
        op = encode_update_operation(FieldType, Op)
      }
              end, Ops),
  EncKey = encode_index_key(Key),
  #apbindexsnestedupdate{key = EncKey, update = EncSubOps};
encode_index_s_single_update({Key, Op}) ->
  encode_index_s_single_update({Key, [Op]}).

decode_index_s_update(#apbindexsupdate{updates = [Update], removes = []}) ->
  {update, decode_indexs_single_update(Update)};
decode_index_s_update(#apbindexsupdate{updates = Updates, removes = []}) ->
  UpdatesDec =
    lists:map(fun(Update) -> decode_indexs_single_update(Update) end, Updates),
  {update, UpdatesDec};
decode_index_s_update(#apbindexsupdate{updates = [], removes = [Key]}) ->
  {remove, decode_index_key(Key)};
decode_index_s_update(#apbindexsupdate{updates = [], removes = Keys}) ->
  DecKeys = lists:map(fun(EncKey) -> decode_index_key(EncKey) end, Keys),
  {remove, DecKeys}.

decode_indexs_single_update({Key, Ops}) when is_list(Ops) ->
  DecSubOps =
    lists:map(fun(SingleUpd) ->
      #apbindexstupleoperation{
        fieldname = FieldName,
        fieldtype = FieldType,
        op = Op
      } = SingleUpd,
      {decode_field(FieldName), decode_type(FieldType), decode_update_operation(Op)}
              end, Ops),
  {Key, DecSubOps};
decode_indexs_single_update({Key, Op}) ->
  decode_indexs_single_update({Key, [Op]}).

encode_field(bound_obj) -> 'BOUND_OBJ';
encode_field(index_val) -> 'INDEX_VAL';
encode_field(F) -> erlang:error({unknown_field_name, F}).

decode_field('BOUND_OBJ') -> bound_obj;
decode_field('INDEX_VAL') -> index_val;
decode_field(F) -> erlang:error({unknown_field_name_protobuf, F}).

% index responses
%% index_p

encode_index_get_resp(Entries) ->
  #apbgetindexresp{entries = [encode_index_entry(E) || E <- Entries]}.

decode_index_get_resp(#apbgetindexresp{entries = Entries}) ->
  [decode_index_entry(E) || E <- Entries].

encode_index_entry({Key, BoundObjs}) when is_list(BoundObjs) ->
  #apbindexentry{
    key = encode_index_key(Key),
    boundobjs = [encode_bound_object(BObj) || BObj <- BoundObjs]
  };
encode_index_entry({Key, BoundObj}) ->
  encode_index_entry({Key, [BoundObj]}).

decode_index_entry(#apbindexentry{key = KeyEnc, boundobjs = BObjsEnc}) ->
  Key = decode_index_key(KeyEnc),
  {Key, [decode_bound_object(BObj) || BObj <- BObjsEnc]}.

-ifdef(TEST).

%% Tests encode and decode
start_transaction_test() ->
    Clock = term_to_binary(ignore),
    Properties = [],
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties}),
    ?assertMatch(true, is_record(EncRecord, apbstarttransaction)),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)).

%% Tests encode and decode
start_transaction_properties_1_test() ->
    Clock = term_to_binary(ignore),
    Properties = [{certify,dont_certify},{locks,[<<"lock1">>,<<"lock2">>]},{update_clock,true}],
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties}),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)),
    ?assertMatch({ok,[<<"lock1">>,<<"lock2">>]},orddict:find(locks,antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties))).
start_transaction_properties_2_test() ->
    Clock = term_to_binary(ignore),
    Properties = [{locks,[<<"lock1">>,<<"lock2">>]},{update_clock,true}],
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties}),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)).
start_transaction_properties_3_test() ->
    Clock = term_to_binary(ignore),
    Properties = [{certify,dont_certify},{update_clock,true}],
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties}),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)).
start_transaction_properties_4_test() ->
    Clock = term_to_binary(ignore),
    Properties = [{certify,dont_certify},{locks,[<<"lock1">>,<<"lock2">>]}],
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties}),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)).
start_transaction_properties_5_test() ->
    Clock = term_to_binary(ignore),
    Properties = [{certify,dont_certify},{shared_locks,[<<"lock1">>,<<"lock2">>]}],
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties}),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)).
start_transaction_properties_6_test() ->
    Clock = term_to_binary(ignore),
    Properties = [{certify,dont_certify},{exclusive_locks,[<<"lock1">>,<<"lock2">>]}],
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties}),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)).


read_transaction_test() ->
  Objects = [{<<"key1">>, antidote_crdt_counter_pn, <<"bucket1">>},
    {<<"key2">>, antidote_crdt_set_aw, <<"bucket2">>}],
  TxId = term_to_binary({12}),
  %% Dummy value, structure of TxId is opaque to client
  EncRecord = antidote_pb_codec:encode_read_objects(Objects, TxId),
  ?assertMatch(true, is_record(EncRecord, apbreadobjects)),
  [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
  Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
  ?assertMatch(true, is_record(Msg, apbreadobjects)),
  DecObjects = lists:map(fun(O) ->
    antidote_pb_codec:decode_bound_object(O) end,
    Msg#apbreadobjects.boundobjects),
  ?assertMatch(Objects, DecObjects),
  %% Test encoding error
  ErrEnc = antidote_pb_codec:encode(read_objects_response,
    {error, someerror}),
  [ErrMsgCode, ErrMsgData] = riak_pb_codec:encode(ErrEnc),
  ErrMsg = riak_pb_codec:decode(ErrMsgCode, list_to_binary(ErrMsgData)),
  ?assertMatch({error, unknown},
    antidote_pb_codec:decode_response(ErrMsg)),

  %% Test encoding results
  Results = [1, [<<"a">>, <<"b">>]],
  ResEnc = antidote_pb_codec:encode(read_objects_response,
    {ok, lists:zip(Objects, Results)}
  ),
  [ResMsgCode, ResMsgData] = riak_pb_codec:encode(ResEnc),
  ResMsg = riak_pb_codec:decode(ResMsgCode, list_to_binary(ResMsgData)),
  ?assertMatch({read_objects, [{counter, 1}, {set, [<<"a">>, <<"b">>]}]},
    antidote_pb_codec:decode_response(ResMsg)).

update_types_test() ->
  Updates = [{{<<"1">>, antidote_crdt_counter_pn, <<"2">>}, increment, 1},
    {{<<"2">>, antidote_crdt_counter_pn, <<"2">>}, increment, 1},
    {{<<"a">>, antidote_crdt_set_aw, <<"2">>}, add, <<"3">>},
    {{<<"b">>, antidote_crdt_counter_pn, <<"2">>}, increment, 2},
    {{<<"c">>, antidote_crdt_set_aw, <<"2">>}, add, <<"4">>},
    {{<<"a">>, antidote_crdt_set_aw, <<"2">>}, add_all, [<<"5">>, <<"6">>]}
  ],
  TxId = term_to_binary({12}),
  %% Dummy value, structure of TxId is opaque to client
  EncRecord = antidote_pb_codec:encode_update_objects(Updates, TxId),
  ?assertMatch(true, is_record(EncRecord, apbupdateobjects)),
  [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
  Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
  ?assertMatch(true, is_record(Msg, apbupdateobjects)),
  DecUpdates = lists:map(fun(O) ->
    antidote_pb_codec:decode_update_op(O) end,
    Msg#apbupdateobjects.updates),
  ?assertMatch(Updates, DecUpdates).

error_messages_test() ->
  EncRecord1 = antidote_pb_codec:encode(start_transaction_response,
    {error, someerror}),
  [MsgCode1, MsgData1] = riak_pb_codec:encode(EncRecord1),
  Msg1 = riak_pb_codec:decode(MsgCode1, list_to_binary(MsgData1)),
  Resp1 = antidote_pb_codec:decode_response(Msg1),
  ?assertMatch(Resp1, {error, unknown}),

  EncRecord2 = antidote_pb_codec:encode(operation_response,
    {error, someerror}),
  [MsgCode2, MsgData2] = riak_pb_codec:encode(EncRecord2),
  Msg2 = riak_pb_codec:decode(MsgCode2, list_to_binary(MsgData2)),
  Resp2 = antidote_pb_codec:decode_response(Msg2),
  ?assertMatch(Resp2, {error, unknown}),

  EncRecord3 = antidote_pb_codec:encode(read_objects_response,
    {error, someerror}),
  [MsgCode3, MsgData3] = riak_pb_codec:encode(EncRecord3),
  Msg3 = riak_pb_codec:decode(MsgCode3, list_to_binary(MsgData3)),
  Resp3 = antidote_pb_codec:decode_response(Msg3),
  ?assertMatch(Resp3, {error, unknown}).

-define(TestCrdtOperationCodec(Type, Op, Param),
  ?assertEqual(
    {{<<"key">>, Type, <<"bucket">>}, Op, Param},
    decode_update_op(encode_update_op({<<"key">>, Type, <<"bucket">>}, Op, Param)))
).

-define(TestCrdtResponseCodec(Type, ExpectedType, Val),
  ?assertEqual(
    {ExpectedType, Val},
    decode_read_object_resp(encode_read_object_resp(Type, Val)))
).

crdt_encode_decode_test() ->
  %% encoding the following operations and decoding them again, should give the same result

  % Counter
  ?TestCrdtOperationCodec(antidote_crdt_counter_pn, increment, 1),
  ?TestCrdtResponseCodec(antidote_crdt_counter_pn, counter, 42),

  % lww-register
  ?TestCrdtOperationCodec(antidote_crdt_register_lww, assign, <<"hello">>),
  ?TestCrdtResponseCodec(antidote_crdt_register_lww, reg, <<"blub">>),


  % mv-register
  ?TestCrdtOperationCodec(antidote_crdt_register_mv, assign, <<"hello">>),
  ?TestCrdtResponseCodec(antidote_crdt_register_mv, mvreg, [<<"a">>, <<"b">>, <<"c">>]),

  % set
  ?TestCrdtOperationCodec(antidote_crdt_set_aw, add, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_aw, add_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtOperationCodec(antidote_crdt_set_aw, remove, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_aw, remove_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtResponseCodec(antidote_crdt_set_aw, set, [<<"a">>, <<"b">>, <<"c">>]),

  % same for remove wins set:
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, add, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, add_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, remove, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, remove_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtResponseCodec(antidote_crdt_set_rw, set, [<<"a">>, <<"b">>, <<"c">>]),

  % map
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, update, {{<<"key">>, antidote_crdt_register_mv}, {assign, <<"42">>}}),
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, update, [
    {{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}},
    {{<<"b">>, antidote_crdt_set_aw}, {add, <<"x">>}}]),
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, remove, {<<"key">>, antidote_crdt_register_mv}),
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, remove, [
    {<<"a">>, antidote_crdt_register_mv},
    {<<"b">>, antidote_crdt_register_mv}]),
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, batch, {
    [{{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}},
      {{<<"b">>, antidote_crdt_set_aw}, {add, <<"x">>}}],
    [{<<"a">>, antidote_crdt_register_mv},
      {<<"b">>, antidote_crdt_register_mv}]}),

  ?TestCrdtResponseCodec(antidote_crdt_map_rr, map, [
    {{<<"a">>, antidote_crdt_register_mv}, <<"42">>}
  ]),

  % gmap
  ?TestCrdtOperationCodec(antidote_crdt_map_go, update, {{<<"key">>, antidote_crdt_register_mv}, {assign, <<"42">>}}),
  ?TestCrdtOperationCodec(antidote_crdt_map_go, update, [
    {{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}},
    {{<<"b">>, antidote_crdt_set_aw}, {add, <<"x">>}}]),
  ?TestCrdtResponseCodec(antidote_crdt_map_go, map, [
    {{<<"a">>, antidote_crdt_register_mv}, <<"42">>}
  ]),

  % flag
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, enable, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, disable, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, reset, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, enable, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, disable, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, reset, {}),

  % index
  ?TestCrdtOperationCodec(antidote_crdt_index_p, update, {<<"key">>, {assign, <<"value">>}}),
  ?TestCrdtOperationCodec(antidote_crdt_index_p, remove, {<<"key">>}),
  ?TestCrdtOperationCodec(antidote_crdt_index, update, {<<"key">>, [
    {bound_obj, antidote_crdt_register_lww, {assign, {<<"key">>, antidote_crdt_map_go, <<"bucket">>}}},
    {index_val, antidote_crdt_register_lww, {assign, <<"value">>}}]}),
  ?TestCrdtResponseCodec(antidote_crdt_index, index, [{<<"key">>, [
    {field1, antidote_crdt_register_lww, {assign, boundobj}}]}]),

  ok.



-endif.




