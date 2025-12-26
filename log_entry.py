from dataclasses import dataclass
from typing import Optional

@dataclass
class LogEntry():
    term: int
    key: Optional[str] = None
    value: Optional[str] = None
