from log_entry import LogEntry
import raft_pb2


def to_proto_entry(e: LogEntry) -> raft_pb2.LogEntry:
    return raft_pb2.LogEntry(term=e.term, key=e.key or "", value=e.value or "")

def from_proto_entry(p: raft_pb2.LogEntry) -> LogEntry:
    return LogEntry(term=p.term, key=p.key, value=p.value)
