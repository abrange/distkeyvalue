import os
import json
from dataclasses import asdict

from log_entry import LogEntry



class RaftStorage():

    def __init__(self, file_name:str):

        self.file_name = file_name
    
    def save(self, current_term: int, vote_for: str, commit_index: int, log_entries: list['LogEntry']):

        data = {
            "current_term": current_term,
            "vote_for": vote_for,
            "commit_index": commit_index,
            "log": [asdict(entry) for entry in log_entries]
        }

        tmp_file_name = self.file_name + ".tmp"
        with open(tmp_file_name, "w") as f:
            json.dump(data, f)
            f.flush()   # write buffers
            # f.fileno returns the file descriptors
            os.fsync(f.fileno())
        os.replace(tmp_file_name, self.file_name)
        print(f"File {self.file_name} written to disk")

    def load(self) -> (int, str, list['LogEntry']):

        if not os.path.exists(self.file_name):
            return 0, None, 0, []
        
        with open(self.file_name, "r") as f:
            data = json.load(f)

        from raft_lab import LogEntry
        # rebuild log
        restored_log = [
            LogEntry(term=e["term"],
            key=e["key"],
            value=e["value"],
            )
            for e in data.get("log", [])
        ]
        print(f"Data read from {self.file_name}")
        return data["current_term"], data["vote_for"], data["commit_index"], restored_log
