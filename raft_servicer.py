from log_entry import LogEntry
import raft_pb2, raft_pb2_grpc
from rpc_codec import from_proto_entry

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def __init__(self, node):
        self.node = node

    async def RequestVote(self, request, context):
        term, ok = await self.node.on_request_vote(
            candidate_term=request.term,
            candidate_id=request.candidate_id,
            candidate_last_idx=request.last_idx,
            candidate_last_term=request.last_term,
        )
        return raft_pb2.RequestVoteReply(term=term, vote_granted=ok)

    async def AppendEntries(self, request, context):
        entries = [from_proto_entry(e) for e in request.entries]
        term, ok = await self.node.on_append_entries(
            term=request.term,
            leader_id=request.leader_id,
            prev_idx=request.prev_idx,
            prev_term=request.prev_term,
            entries=entries,
            leader_commit=request.leader_commit,
        )
        return raft_pb2.AppendEntriesReply(term=term, success=ok)

    async def InstallSnapshot(self, request, context):
        term, ok = await self.node.on_install_snapshot(
            term=request.term,
            leader_id=request.leader_id,
            snap_last_idx=request.snap_last_idx,
            snap_last_term=request.snap_last_term,
            kv=dict(request.kv),
        )
        return raft_pb2.InstallSnapshotReply(term=term, success=ok)
