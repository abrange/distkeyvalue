from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from raft_node import Role

class PutBody(BaseModel):
    value: str

def make_app(node, leader_addr_map: dict[str, str]):
    app = FastAPI()

    @app.get("/kv/{key}")
    async def get_key(key: str):
        if node.role != Role.LEADER:
            leader_id = getattr(node, "leader_id", None)
            raise HTTPException(
                status_code=409,
                detail={"error": "not_leader", "leader_id": leader_id,
                        "leader_addr": leader_addr_map.get(leader_id) if leader_id else None}
            )

        val, ok = await node.get_value(key)
        if not ok:
            raise HTTPException(status_code=503, detail={"error": "leader_not_confirmed"})
        return {"ok": True, "key": key, "value": val}

    @app.put("/kv/{key}")
    async def put_key(key: str, body: PutBody):
        if node.role != Role.LEADER:
            leader_id = getattr(node, "leader_id", None)
            raise HTTPException(
                status_code=409,
                detail={"error": "not_leader", "leader_id": leader_id,
                        "leader_addr": leader_addr_map.get(leader_id) if leader_id else None}
            )

        ok = await node.propose_put(key, body.value)
        if not ok:
            raise HTTPException(status_code=503, detail={"error": "put_failed"})
        return {"ok": True}

    return app
