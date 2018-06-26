% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(mem3_nodes).
-behaviour(gen_server).
-behaviour(config_listener).
-vsn(1).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-export([start_link/0, get_nodelist/0, get_node_info/2]).
-export([handle_config_change/5, handle_config_terminate/3]).


-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-record(state, {changes_pid, update_seq}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_nodelist() ->
    try
        lists:sort([N || {N,_} <- ets:tab2list(?MODULE)])
    catch error:badarg ->
        gen_server:call(?MODULE, get_nodelist)
    end.

get_node_info(Node, Key) ->
    try
        couch_util:get_value(Key, ets:lookup_element(?MODULE, Node, 2))
    catch error:badarg ->
        gen_server:call(?MODULE, {get_node_info, Node, Key})
    end.

init([]) ->
    ets:new(?MODULE, [named_table, {read_concurrency, true}]),
    UpdateSeq = initialize_nodelist(),
    ok = config:listen_for_changes(?MODULE, nil),
    {Pid, _} = spawn_monitor(fun() -> listen_for_changes(UpdateSeq) end),
    {ok, #state{changes_pid = Pid, update_seq = UpdateSeq}}.

handle_call(get_nodelist, _From, State) ->
    {reply, lists:sort([N || {N,_} <- ets:tab2list(?MODULE)]), State};
handle_call({get_node_info, Node, Key}, _From, State) ->
    Resp = try
        couch_util:get_value(Key, ets:lookup_element(?MODULE, Node, 2))
    catch error:badarg ->
        error
    end,
    {reply, Resp, State};
handle_call({add_node, Node, NodeInfo}, _From, State) ->
    gen_event:notify(mem3_events, {add_node, Node}),
    ets:insert(?MODULE, {Node, NodeInfo}),
    {reply, ok, State};
handle_call({remove_node, Node}, _From, State) ->
    gen_event:notify(mem3_events, {remove_node, Node}),
    ets:delete(?MODULE, Node),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _, _, Pid, Reason}, #state{changes_pid=Pid} = State) ->
    couch_log:notice("~p changes listener died ~p", [?MODULE, Reason]),
    StartSeq = State#state.update_seq,
    Seq = case Reason of {seq, EndSeq} -> EndSeq; _ -> StartSeq end,
    erlang:send_after(5000, self(), start_listener),
    {noreply, State#state{update_seq = Seq}};
handle_info(start_listener, #state{update_seq = Seq} = State) ->
    {NewPid, _} = spawn_monitor(fun() -> listen_for_changes(Seq) end),
    {noreply, State#state{changes_pid=NewPid}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, #state{}=State, _Extra) ->
    {ok, State}.

handle_config_change("location", Key, Value, _, State) ->
    update_metadata([<<"location">>, ?l2b(Key)], convert_val(Value)),
    {ok, State};
handle_config_change(_, _, _, _, State) ->
    {ok, State}.

handle_config_terminate(_Server, _Reason, _State) ->
    ok.

%% internal functions

initialize_nodelist() ->
    DbName = config:get("mem3", "nodes_db", "_nodes"),
    {ok, Db} = mem3_util:ensure_exists(DbName),
    SelfId = couch_util:to_binary(node()),
    {ok, {_, _, Doc}} = couch_db:fold_docs(Db, fun first_fold/2, {Db, SelfId, nil}, []),
    case Doc of
    nil ->
        {NodeProps} = merge_config({[]}, ["location"]),
        ets:insert(?MODULE, {node(), NodeProps}),
        NewDoc = #doc{id = SelfId, body = {NodeProps}},
        {ok, _} = couch_db:update_doc(Db, NewDoc, []);
    #doc{id = SelfId, body = {DocProps}} = Doc ->
        {NewProps} = merge_config({DocProps}, ["location"]),
        if NewProps =/= DocProps ->
            couch_db:update_doc(Db, Doc#doc{body = {NewProps}});
        true ->
            ok
        end
    end,
    Seq = couch_db:get_update_seq(Db),
    couch_db:close(Db),
    Seq.

first_fold(#full_doc_info{id = <<"_design/", _/binary>>}, Acc) ->
    {ok, Acc};
first_fold(#full_doc_info{deleted=true}, Acc) ->
    {ok, Acc};
first_fold(#full_doc_info{id=Id}=DocInfo, {Db, SelfId, DocAcc}) ->
    {ok, Doc} = couch_db:open_doc(Db, DocInfo, [ejson_body]),
    #doc{body = {Props}} = Doc,
    ets:insert(?MODULE, {mem3_util:to_atom(Id), Props}),
    if Id =:= SelfId ->
        {ok, {Db, SelfId, Doc}};
    true ->
        {ok, {Db, SelfId, DocAcc}}
    end.

listen_for_changes(Since) ->
    DbName = config:get("mem3", "nodes_db", "_nodes"),
    {ok, Db} = mem3_util:ensure_exists(DbName),
    Args = #changes_args{
        feed = "continuous",
        since = Since,
        heartbeat = true,
        include_docs = true
    },
    ChangesFun = couch_changes:handle_db_changes(Args, nil, Db),
    ChangesFun(fun changes_callback/2).

changes_callback(start, _) ->
    {ok, nil};
changes_callback({stop, EndSeq}, _) ->
    exit({seq, EndSeq});
changes_callback({change, {Change}, _}, _) ->
    Node = couch_util:get_value(<<"id">>, Change),
    case Node of <<"_design/", _/binary>> -> ok; _ ->
        case mem3_util:is_deleted(Change) of
        false ->
            {Props} = couch_util:get_value(doc, Change),
            gen_server:call(?MODULE, {add_node, mem3_util:to_atom(Node), Props});
        true ->
            gen_server:call(?MODULE, {remove_node, mem3_util:to_atom(Node)})
        end
    end,
    {ok, couch_util:get_value(<<"seq">>, Change)};
changes_callback(timeout, _) ->
    {ok, nil}.

update_metadata(KeyPath, Value) ->
    DbName = config:get("mem3", "nodes_db", "_nodes"),
    {ok, Db} = mem3_util:ensure_exists(DbName),
    try
        {ok, Doc} = couch_db:open_doc(Db, couch_util:to_binary(node()), [ejson_body]),
        #doc{body = {DocBody}} = Doc,
        case merge_field(DocBody, KeyPath, Value) of
        {DocBody} ->
            % internal doc state matches .ini
            ok;
        {MergedBody} ->
            NewDoc = Doc#doc{body = {MergedBody}},
            {ok, _} = couch_db:update_doc(Db, NewDoc, [])
        end
    after
        couch_db:close(Db)
    end.

merge_config({OldProps}, Sections) ->
    lists:foldl(fun(Section, {OuterAcc}) ->
        lists:foldl(fun({Key, Value}, {InnerAcc}) ->
            merge_field(InnerAcc, [?l2b(Section), ?l2b(Key)], convert_val(Value))
        end, {OuterAcc}, config:get(Section))
    end, {OldProps}, Sections).

merge_field(DocProps, [LastKey], Value) ->
    {lists:ukeymerge(1, [{LastKey, Value}], lists:keysort(1, DocProps))};
merge_field(DocProps, [Key | ChildPath], Value) ->
    case lists:keytake(Key, 1, DocProps) of
        false ->
            {[{Key, merge_field([], ChildPath, Value)} | DocProps]};
        {value, {Key, {Nest}}, OtherProps} when is_list(Nest) ->
            {[{Key, merge_field(Nest, ChildPath, Value)} | OtherProps]};
        {value, OtherType, OtherProps} ->
            Msg = "Unexpected content in _nodes DB, skipping config merge",
            couch_log:error(Msg, []),
            {DocProps}
    end.

convert_val("true") ->
    true;
convert_val("false") ->
    false;
convert_val(Else) ->
    ?l2b(Else).
