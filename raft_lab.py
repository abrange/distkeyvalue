from ast import Tuple
import asyncio
import random
import grpc
from grpc import aio
from grpc_transport import GrpcTransport
import raft_pb2_grpc

from enum import Enum
from re import L
from tkinter import NO
from typing import Optional, List
from dataclasses import dataclass

from log_entry import LogEntry
from raft_node import RaftNode, Role
from raft_servicer import RaftServicer
from raft_storage import RaftStorage
from snapshot_storage import SnapshotStorage

ADDRS = {
    "A": "127.0.0.1:50051",
    "B": "127.0.0.1:50052",
    "C": "127.0.0.1:50053",
}

async def start_server(node_id: str, node):
    server = aio.server()
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(node), server)
    server.add_insecure_port(ADDRS[node_id])
    await server.start()
    return server



def create_node(node_id: str, peers: List[str], transport):

    storage = RaftStorage(f"data/raft_{node_id}.json")
    n = RaftNode(node_id,peers, storage, transport=transport)
    return n

async def main():
    all_nodes = "ABC"

    # 1) Create nodes with gRPC transports (each node has stubs to peers)
    nodes = {}
    for i in all_nodes:
        peer_addrs = {j: ADDRS[j] for j in all_nodes if j != i}
        transport = GrpcTransport(peer_addrs)
        nodes[i] = create_node(i, [j for j in all_nodes if j != i], transport)

    # 2) Start gRPC servers (so they can receive RPCs)
    servers = []
    for i in all_nodes:
        servers.append(await start_server(i, nodes[i]))

    # 3) Start Raft background loops (election timers, etc.)
    asyncio.gather(*[asyncio.create_task(node.run()) for node in nodes.values()])

    await asyncio.sleep(5)

    leader = next(n for n in nodes.values() if n.role == Role.LEADER)
    print(f"Leader is {leader.node_id}")

    assert await leader.propose_put("K1", "1")
    assert await leader.propose_put("K2", "2")
    assert await leader.propose_put("K3", "3")
    assert await leader.propose_put("K4", "4")
    assert await leader.propose_put("K5", "5")
    assert await leader.propose_put("K6", "6")

    print("Get K1", await leader.get_value("K1"))

    await asyncio.sleep(1)
    for n_id, node in nodes.items():
        print(n_id, node.role.value, node.commit_index, node.kv)

    # 4) Cleanup
    for s in servers:
        await s.stop(grace=None)



async def serve(node, host, port):
    server = aio.server()
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(node), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    return server



if __name__ == "__main__":
    asyncio.run(main())