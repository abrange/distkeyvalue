import asyncio
from grpc import aio
import grpc
import raft_pb2, raft_pb2_grpc
from rpc_codec import to_proto_entry

class GrpcTransport:
    def __init__(self, peer_addrs: dict[str, str]):
        self.peer_addrs = peer_addrs
        self.channels = {pid: aio.insecure_channel(addr) for pid, addr in peer_addrs.items()}
        self.stubs = {pid: raft_pb2_grpc.RaftStub(ch) for pid, ch in self.channels.items()}

    async def request_vote(self, target, term, candidate_id, last_idx, last_term):
        req = raft_pb2.RequestVoteRequest(term=term, candidate_id=candidate_id, last_idx=last_idx, last_term=last_term)
        try:
            resp = await self.stubs[target].RequestVote(req, timeout=0.3)
            return resp.term, resp.vote_granted
        except (grpc.aio.AioRpcError, asyncio.TimeoutError, OSError):
            # peer down / timeout / connection refused
            return term, False

    async def append_entries(self, target, term, leader_id, prev_idx, prev_term, entries, leader_commit):
        proto_entries = [to_proto_entry(e) for e in entries]
        req = raft_pb2.AppendEntriesRequest(
            term=term, leader_id=leader_id, prev_idx=prev_idx, prev_term=prev_term,
            entries=proto_entries, leader_commit=leader_commit
        )
        try:
            resp = await self.stubs[target].AppendEntries(req, timeout=0.3)
            return resp.term, resp.success
        except (grpc.aio.AioRpcError, asyncio.TimeoutError, OSError):
            return term, False

    async def install_snapshot(self, target, term, leader_id, snap_last_idx, snap_last_term, kv):
        req = raft_pb2.InstallSnapshotRequest(
            term=term, leader_id=leader_id, snap_last_idx=snap_last_idx, snap_last_term=snap_last_term, kv=kv
        )
        try:    
            resp = await self.stubs[target].InstallSnapshot(req, timeout=0.8)
            return resp.term, resp.success
        except (grpc.aio.AioRpcError, asyncio.TimeoutError, OSError):
            return term, False
