import argparse
import asyncio
import os

import uvicorn
from grpc import aio

import raft_pb2_grpc

from raft_node import RaftNode
from raft_servicer import RaftServicer
from grpc_transport import GrpcTransport
from raft_storage import RaftStorage
from snapshot_storage import SnapshotStorage
from http_api import make_app  # your FastAPI wrapper


def parse_map(s: str) -> dict[str, str]:
    """
    Parse: "B=127.0.0.1:50052,C=127.0.0.1:50053" -> {"B": "...", "C": "..."}
    """
    out = {}
    if not s:
        return out
    parts = [p.strip() for p in s.split(",") if p.strip()]
    for p in parts:
        k, v = p.split("=", 1)
        out[k.strip()] = v.strip()
    return out


async def main_async(args):
    os.makedirs(args.data_dir, exist_ok=True)

    # --- storage per node ---
    raft_state_path = os.path.join(args.data_dir, "raft_state.json")
    snapshot_path = os.path.join(args.data_dir, "snapshot.json")
    storage = RaftStorage(raft_state_path)
    snapstore = SnapshotStorage(snapshot_path)

    # --- parse peers ---
    grpc_peers = parse_map(args.grpc_peers)     # { "B": "127.0.0.1:50052", ... }
    http_peers = parse_map(args.http_peers)     # { "B": "http://127.0.0.1:8002", ... }

    peers = sorted(grpc_peers.keys())

    # --- transport for node-to-node ---
    transport = GrpcTransport(grpc_peers)

    # --- create raft node ---
    # Adjust constructor to match your codebase:
    # - If you pass storage/snapstore in ctor, do it.
    node = RaftNode(args.node_id, peers=peers, storage=storage, transport=transport)
    #node.snapstore = snapstore

    # Optional but helpful for redirects in HTTP API:
    node.leader_http_map = http_peers

    # --- start gRPC server ---
    grpc_server = aio.server()
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(node), grpc_server)
    grpc_server.add_insecure_port(f"{args.host}:{args.grpc_port}")
    await grpc_server.start()
    print(f"[{args.node_id}] gRPC listening on {args.host}:{args.grpc_port}")

    # --- start HTTP server (FastAPI) ---
    app = make_app(node, leader_addr_map=http_peers)
    uv_config = uvicorn.Config(app, host=args.host, port=args.http_port, log_level="info")
    uv_server = uvicorn.Server(uv_config)

    # --- start raft background loop + http loop ---
    raft_task = asyncio.create_task(node.run(), name=f"raft-{args.node_id}")
    http_task = asyncio.create_task(uv_server.serve(), name=f"http-{args.node_id}")

    try:
        # keep process alive
        done, pending = await asyncio.wait([raft_task, http_task], return_when=asyncio.FIRST_EXCEPTION)
        for t in done:
            if t.exception():
                print("Task crashed:", t.get_name(), t.exception())
    finally:
        # graceful shutdown
        try:
            if hasattr(node, "stop"):
                await node.stop()
        except Exception:
            pass
        await grpc_server.stop(grace=0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node-id", required=True, help="A/B/C")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--grpc-port", type=int, required=True)
    ap.add_argument("--http-port", type=int, required=True)
    ap.add_argument("--grpc-peers", required=True,
                    help='e.g. "B=127.0.0.1:50052,C=127.0.0.1:50053" (exclude self)')
    ap.add_argument("--http-peers", required=True,
                    help='e.g. "A=http://127.0.0.1:8001,B=http://127.0.0.1:8002,C=http://127.0.0.1:8003"')
    ap.add_argument("--data-dir", required=True, help="Per-node data dir, e.g. data/A")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
