import os
import json

class SnapshotStorage:
    def __init__(self, filename:str):
        self.filename: str = filename
    
    def save(
        self,
        snap_last_idx: int,
        snap_last_term: int,
        kv: dict,
        ):

        # Create a temporal file to do save in an atomic way
        tmp_file = self.filename + ".tmp"
        with open(tmp_file, "w") as f:
            data = {
                "snap_last_idx": snap_last_idx,
                "snap_last_term": snap_last_term,
                "data": kv
            }

            json.dump(data, f)
            # Pushes Python's in-memory buffer to the OS (Kernel)
            f.flush()
            # After data leaves Python, it may be in OS page cache
            # fsync ask the OS to flush its page cache to the file storage
            os.fsync(f.fileno())
        
        # Atomic file replacement to avoid incosistent status
        os.replace(tmp_file, self.filename)
    
    def load(self) -> (int, int, dict):

        if not os.path.exists(self.filename):
            return (0, 0, {})
        
        with open(self.filename, "r") as f:
            d = json.load(f)
            return (
                d["snap_last_idx"],
                d["snap_last_term"],
                d["data"]
            )

