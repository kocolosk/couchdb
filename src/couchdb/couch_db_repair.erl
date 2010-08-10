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

-module(couch_db_repair).

-compile(export_all).
-export([repair/1, merge_to_file/2, make_lost_and_found/1,
         make_lost_and_found/3, find_nodes_quickly/1]).

-include("couch_db.hrl").

-define(CHUNK_SIZE, 1048576).
-define(SIZE_BLOCK, 4096).

repair(DbName) ->
    RootDir = couch_config:get("couchdb", "database_dir", "."),
    FullPath = filename:join([RootDir, "./" ++ DbName ++ ".couch"]),
    {ok, Fd} = couch_file:open(FullPath, []),
    Ret = maybe_add_missing_header(Fd),
    couch_file:close(Fd),
    % TODO: log if return value matches {ok, repaired, _BTreeInfos}
    Ret.


maybe_add_missing_header(Fd) ->
    case couch_file:read_header(Fd, [return_pos]) of
    no_valid_header ->
        no_header; % TODO: ignore?
    {ok, Header, HeaderPos} ->
        case find_last_btree_root(Fd, by_seq) of
        {nil, _} ->
            ok;
        {_, NodePos, _, _} = BySeqInfo when NodePos > HeaderPos ->
            ByIdInfo = find_last_btree_root(Fd, by_id),
            add_missing_header(
                Fd, Header,
                sort_btree_infos(ByIdInfo, BySeqInfo)
            );
        _ ->
            ok
        end
     end.


sort_btree_infos({Root1Node, Root1Pos, BTree1Type, LastKey1},
    {Root2Node, Root2Pos, BTree2Type, LastKey2}) ->
    case {BTree1Type, BTree2Type} of
    {by_seq, by_id} ->
        {by_seq, LastKey1, Root1Pos, Root1Node,
            by_id, LastKey2, Root2Pos, Root2Node};
    {by_id, by_seq} ->
        {by_seq, LastKey2, Root2Pos, Root2Node,
            by_id, LastKey1, Root1Pos, Root1Node}
    end.

add_missing_header(Fd, LastHeader, BTreeInfos) ->
    {by_seq, BySeqLastKey, BySeqRootPos, _BySeqRootNode,
        by_id, _ByIdLastKey, ByIdRootPos, _ByIdRootNode} = BTreeInfos,
    {_OldBySeqOffset, OldBySeqRed} =
        LastHeader#db_header.docinfo_by_seq_btree_state,
    {_OldByIdOffset, OldByIdRed} =
        LastHeader#db_header.fulldocinfo_by_id_btree_state,
    NewHeader = LastHeader#db_header{
        update_seq = BySeqLastKey,
        fulldocinfo_by_id_btree_state = {ByIdRootPos, OldByIdRed},
        docinfo_by_seq_btree_state = {BySeqRootPos, OldBySeqRed}
    },
    ok = couch_file:write_header(Fd, NewHeader),
    ok = couch_file:sync(Fd),
    {ok, repaired, BTreeInfos}.


find_last_btree_root(Fd, Type) ->
    {ok, StartPos} = couch_file:bytes(Fd),
    find_last_btree_root(Fd, Type, StartPos).


find_last_btree_root(_Fd, _Type, Pos) when Pos < 0 ->
    {nil, -1};
find_last_btree_root(Fd, BTreeType, Pos) ->
    case couch_file:pread_term(Fd, Pos) of
    {ok, {kv_node, _} = Node} ->
        {Type, LastKey} = btree_type(Pos, Fd),
        case Type of
        BTreeType ->
            {Node, Pos, Type, LastKey};
        _Other ->
            find_last_btree_root(Fd, BTreeType, Pos - 1)
        end;
    {ok, {kp_node, _} = Node} ->
        {Type, LastKey} = btree_type(Pos, Fd),
        case Type of
        BTreeType ->
            {Node, Pos, Type, LastKey};
        _Other ->
            find_last_btree_root(Fd, BTreeType, Pos - 1)
        end;
    _ ->
        find_last_btree_root(Fd, BTreeType, Pos - 1)
    end.


btree_type(NodePos, _Fd) when NodePos < 0 ->
    nil;
btree_type(NodePos, Fd) ->
    {ok, Btree} = couch_btree:open({NodePos, 0}, Fd, []),
    {ok, _, {LastKey, _}} = couch_btree:fold(
        Btree,
        fun(KV, _, _) -> {stop, KV} end,
        ok,
        [{dir, rev}]
    ),
    case key_type(LastKey) of
    integer ->
        {by_seq, LastKey};
    binary ->
        {by_id, LastKey}
    end.


key_type(K) when is_integer(K) ->
    integer;
key_type(K) when is_binary(K) ->
    binary.

merge_to_file(Db, TargetName) ->
    Options = [{user_ctx, #user_ctx{roles = [<<"_admin">>]}}],
    case couch_db:open(TargetName, Options) of
    {ok, TargetDb0} ->
        ok;
    {not_found, no_db_file} ->
        {ok, TargetDb0} = couch_db:create(TargetName, Options)
    end,
    TargetDb = TargetDb0#db{fsync_options = [before_header]},

    {ok, _, {_, FinalDocs}} =
    couch_btree:fold(Db#db.fulldocinfo_by_id_btree, fun(FDI, _, {I, Acc}) ->
        #doc_info{id=Id, revs = RevsInfo} = couch_doc:to_doc_info(FDI),
        LeafRevs = [Rev || #rev_info{rev=Rev} <- RevsInfo],
        {ok, Docs} = couch_db:open_doc_revs(Db, Id, LeafRevs, [latest]),
        if I > 1000 ->
            couch_db:update_docs(TargetDb, [Doc || {ok, Doc} <- Acc],
                [full_commit], replicated_changes),
            ?LOG_INFO("~p writing ~p updates to ~s", [?MODULE, I, TargetName]),
            {ok, {length(Docs), Docs}};
        true ->
            {ok, {I+length(Docs), Docs ++ Acc}}
        end
    end, {0, []}, []),
    ?LOG_INFO("~p writing ~p updates to ~s", [?MODULE, length(FinalDocs),
        TargetName]),
    couch_db:update_docs(TargetDb, [Doc || {ok, Doc} <- FinalDocs],
        [full_commit], replicated_changes),
    couch_db:close(TargetDb).

make_lost_and_found(DbName) ->
    TargetName = ?l2b(["lost+found/", DbName]),
    RootDir = couch_config:get("couchdb", "database_dir", "."),
    FullPath = filename:join([RootDir, "./" ++ DbName ++ ".couch"]),
    make_lost_and_found(DbName, FullPath, TargetName).

make_lost_and_found(DbName, FullPath, TargetName) ->
    {ok, Fd} = couch_file:open(FullPath, []),
    {ok, Db} = couch_db:open(?l2b(DbName), []),
    BtOptions = [
        {split, fun couch_db_updater:btree_by_id_split/1},
        {join, fun couch_db_updater:btree_by_id_join/2},
        {reduce, fun couch_db_updater:btree_by_id_reduce/3}
    ],
    put(dbname, DbName),
    lists:foreach(fun(Root) ->
        {ok, Bt} = couch_btree:open({Root, 0}, Fd, BtOptions),
        try merge_to_file(Db#db{fulldocinfo_by_id_btree = Bt}, TargetName)
        catch _:Reason ->
            ?LOG_ERROR("~p merge node at ~p ~p", [?MODULE, Root, Reason])
        end
    end, find_nodes_quickly(Fd)).

%% @doc returns a list of offsets in the file corresponding to locations of
%%      all kp and kv_nodes from the by_id tree
find_nodes_quickly(DbName) when is_list(DbName) ->
    RootDir = couch_config:get("couchdb", "database_dir", "."),
    FullPath = filename:join([RootDir, "./" ++ DbName ++ ".couch"]),
    {ok, Fd} = couch_file:open(FullPath, []),
    put(dbname, DbName),
    try find_nodes_quickly(Fd) after couch_file:close(Fd) end;
find_nodes_quickly(Fd) ->
    {ok, EOF} = couch_file:bytes(Fd),
    read_file(Fd, EOF, []).

read_file(Fd, LastPos, Acc) ->
    ChunkSize = erlang:min(?CHUNK_SIZE, LastPos),
    Pos = LastPos - ChunkSize,
    ?LOG_INFO("~p for ~s - scanning ~p bytes at ~p", [?MODULE, get(dbname),
        ChunkSize, Pos]),
    {ok, Data, _} = gen_server:call(Fd, {pread, Pos, ChunkSize}),
    Nodes = read_data(Fd, Data, 0, Pos, []),
    if Pos == 0 ->
        lists:append([Nodes | Acc]);
    true ->
        read_file(Fd, Pos, [Nodes | Acc])
    end.

read_data(Fd, Data, Pos, Offset, Acc0) when Pos < byte_size(Data) ->
    FullOffset = Pos + Offset,
    % look for serialized terms that start with {kv_node,
    <<Pattern:12/binary, _/binary>> = term_to_binary({kv_node, nil}),
    Match = case Data of
    <<_:Pos/binary, Pattern:12/binary, _/binary>> ->
        % the ideal case, a full pattern match
        true;
    <<_:Pos/binary, SplitTerm:13/binary, _/binary>> when
            (FullOffset rem ?SIZE_BLOCK) > (?SIZE_BLOCK - 12) ->
        % check if the term is split across the block boundary
        N = ?SIZE_BLOCK - (FullOffset rem ?SIZE_BLOCK),
        M = 12 - N,
        case SplitTerm of <<Head:N/binary, 0, Tail:M/binary>> ->
            <<Head/binary, Tail/binary>> =:= Pattern;
        _Else ->
            % next block is a header, this can't be a kv_node
            false
        end;
    _ ->
        false
    end,
    Acc = if Match -> node_acc(Fd, FullOffset - 4, Acc0, true); true -> Acc0 end,
    read_data(Fd, Data, Pos+1, Offset, Acc);
read_data(_Fd, _Data, _Pos, _Offset, AccOut) ->
    AccOut.

node_acc(Fd, Pos, Acc, Retry) when Pos >= 0 ->
    case couch_file:pread_term(Fd, Pos) of
    {ok, {_, [{<<"_local/",_/binary>>,_}|_]}} ->
        Acc;
    {ok, {kv_node, [{<<_/binary>>,_}|_]}} ->
        [Pos | Acc];
    {ok, _} ->
        Acc;
    _Error ->
        if Retry, (Pos > 0) -> node_acc(Fd, Pos-1, Acc, false); true -> Acc end
    end;
node_acc(_, _, Acc, _) ->
    Acc.
