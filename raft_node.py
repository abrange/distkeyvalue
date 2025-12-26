from enum import Enum
import asyncio
import random
from typing import List, Optional

from log_entry import LogEntry
from raft_storage import RaftStorage
from snapshot_storage import SnapshotStorage


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode():
    """
    This a Raft Consensus Node implementation
    Paper: https://raft.github.io/raft.pdf
    """
    
    HEARTBEAT_WAIT_TIME: float = 0.2 # 200ms
    SNAP_INTERVAL = 4

    def __init__(self, node_id: str, peers: List[str], storage: RaftStorage, transport):

        self.node_id: str = node_id
        self.peers: List[str] = peers
        self.storage: RaftStorage = storage

        # Load durable data
        term, vote_for, committed_index, log = self.storage.load()
        
        # [Persisted] these following 4 should be saved in permanently/persisted store
        self.current_term: int = term
        # [Persisted] NodeId that has received a vote in current term, or None if any.
        self.voted_for: Optional[str] = vote_for
        # Index of the highest log entry know to be commited?
        # TODO: Difference between commit_index and last_applied?
        # commit_index is value decided by the leader and represent
        # the value know to be commited by the cluster
        # last_applied is the value that has been applied to the State Machine
        # and it is expected that it may lag behind commit_index but just
        # briefly while values are being applied or after a crash or snapshot.
        self.commit_index: int = committed_index

        # [Persisted] Log entries. Each entry contains a command for the state machine and a term when it was received
        self.log: List[LogEntry] = log
        
        # Volatile after restart
        self.role:Role = Role.FOLLOWER
        
        # Index of the highest log entry applied to the state machine 
        self.last_applied: int = 0
        
        self.kv: dict[str, str] = {}
                
        # We keep next index per peer
        # This status is for Leaders and should be re-initialized after election
        self.next_index: dict[str, int] = {}
        # This stores what peer has seen (replicated) on their local Log, but
        # this do not mean the peer has commited these changes yet.
        # self.commit_index contains what Leader know about what has been commited by the cluster on
        # each peer
        self.match_index: dict[str, int] = {}


        # Timing or Tasks related
        # Key idea: the Event object is named like a noun (Sustantivo) (_election_reset),
        # and the method is a verb (Verbo) (reset_election_timer).
        self._election_reset = asyncio.Event()
        self._heartbeat_task = None
        # TODO: What is this
        self._stopped = False
        
        self.transport = transport

        # Snapshot
        self.snapstore: SnapshotStorage = SnapshotStorage(f"data/snap_{self.node_id}.json")
        sidx, sterm, kv = self.snapstore.load()
        
        # Logical lasts index and terms saved on snapshot
        self.snap_last_idx: int = sidx
        self.snap_last_term: int = sterm
        self.leader_id: Optional[str] = None
        self.kv = kv or {}

    def is_leader(self) -> bool:
        return self.role == Role.LEADER
    
    def last_index_term(self) -> (int, int):
        """
        It returns last index and term.
        We will uses Logical Indexes, not real indexes in the log
        """
        if self.log:
            return (self.snap_last_idx + len(self.log), self.log[-1].term)
        return (self.snap_last_idx, self.snap_last_term)



    async def run(self):
        while True:
            # get a timeout between 300 and 500 ms
            timeout = random.uniform(0.30, 0.50)
            try:
                await asyncio.wait_for(self._election_reset.wait(), timeout=timeout)
                # clear reset the internal flag to False
                # so co-routines will be block until flag is set to True
                self._election_reset.clear()
            except asyncio.TimeoutError:
                if self.role != Role.LEADER:
                    await self.start_election()

    
    def step_down(self, new_term):
        print(f"[{self.node_id}] is stepping down as leader")
        self.current_term = new_term
        self.role = Role.FOLLOWER
        self.voted_for = None

    async def get_value(self, key: str) -> tuple[Optional[str], bool]:
        print(f"Getting value for key {key}")
        
        if not self.is_leader():
            #TODO: Redirect to leader
            return None, False

        # 2 confirm that we are still leader by quorum, not that we just "think" that we are the leader
        still_leader : bool = await self._confirm_leadership_quorum()
        if not still_leader or not self.is_leader():
            return None, False
        
        # At this point, self.kv has all values at to commit_index applied
        return self.kv.get(key, None), True


    async def _confirm_leadership_quorum(self) -> bool:
        if not self.is_leader():
            return False
        last_index, last_term = self.last_index_term()

        async def ping(peer) -> bool:
            t, ok = await self.transport.append_entries(
                target=peer,
                term=self.current_term,
                leader_id=self.node_id,
                prev_idx=last_index,
                prev_term=last_term,
                entries=[],         # We are not interested in sending new entries this time
                leader_commit=self.commit_index
            )

            if t > self.current_term:
                self.step_down(t)
                return False
            # ok == True peer has accepted as leader for this term
            return ok
        
        # Ask all peer in parallel
        results = await asyncio.gather(*(ping(p) for p in self.peers))
        
        # Count how many peers counts as their leader
        acks = 1 + sum(1 for r in results if r)

        # Return True if we still have majority
        return acks > self._quorum_size() // 2

    async def _heartbeat_loop(self):

        prev_idx, prev_term = self.last_index_term()
        while self.role == Role.LEADER:
            replies = await asyncio.gather(*(self.transport.append_entries(
                target=p,
                term=self.current_term,
                leader_id=self.node_id,
                prev_idx=self.next_index[p]-1,
                prev_term=prev_term,
                entries=[],
                leader_commit= self.commit_index
            ) for p in self.peers),
            return_exceptions=False)
            
            for t, _ok in replies:
                if t > self.current_term:
                    self.step_down(t)
                    return
            
            await asyncio.sleep(self.HEARTBEAT_WAIT_TIME)
    
    async def stop(self):
        self._stopped = True
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        # TODO: Implement Start / Stop
        # if self.start_election

    async def start_election(self):
        """
        It tries to convert on Leader by sending request vote
        Its increment term and vote by itself
        """
        
        self.role = Role.CANDIDATE
        self.current_term +=1
        self.voted_for = self.node_id

        self.backup()

        print(f"[{self.node_id}] started election for term {self.current_term}")

        last_idx, last_term = self.last_index_term()
        votes = 1

        async def ask(p) -> bool:
            received_term, ok = await self.transport.request_vote(
                target=p,
                term=self.current_term,
                candidate_id=self.node_id,
                last_idx=last_idx,
                last_term=last_term
            )
            # On receving
            # Because this is not completely async, we can skip for now some validations like
            # caurrent role is still Candidate, term is the same as we requested vote for
            if received_term > self.current_term:
                # Oopps, that node is more updated than us, so
                # we switch to be a Follower
                self.role = Role.FOLLOWER
                self.current_term = received_term
            return ok 
        
        results = await asyncio.gather(*(ask(p) for p in self.peers), return_exceptions=True)
        votes += sum([1 for r in results if r])

        if votes > ( 1 + len(self.peers)) // 2:
            self.role = Role.LEADER
            last_idx, _ = self.last_index_term()
            self.next_index = {p: last_idx + 1 for p in self.peers}
            self.match_index = {p: 0 for p in self.peers}
            last_idx = self.snap_last_idx + len(self.log)
            self.next_index = {p: last_idx + 1 for p in self.peers}
            self.match_index = {p: 0 for p in self.peers}
            
            print(f"[{self.node_id}] becomes Leader in term {self.current_term}")
            
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        else:
            # We step down if we do not get all votes
            # I am assume the if condiation would have change ther role
            # to Follower and current_term to th one received by the response
            pass
        

    def _reset_election_timer(self):
        # set the internal flag to True
        # so co-routines won't be blocked
        self._election_reset.set()

    
    async def on_request_vote(self, candidate_term: int, candidate_id: str, candidate_last_idx: int, candidate_last_term: int) -> tuple[str, bool]:
        """"
        # it should return: (my_term, vote granted?)

        """

        if candidate_term < self.current_term:
            # the term of the candidate is older than it own term. Can't vote for it
            print(f"[{self.node_id}] rejected vote because of candidate term < than {self.current_term}")
            return (self.current_term, False)
        
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.voted_for = None
            self.role = Role.FOLLOWER
            self.backup()
        
        # We can't vote for same Candidate within the same Term
        can_vote = self.voted_for is None or self.voted_for == candidate_id

        my_idx, my_term = self.last_index_term()
        # We are checking if Candidate is up to date, if last term > than my term or if it
        # it is equal, last index is received is equal or greather than follower
        # The Raft rule is lexicographic:
        # If the candidate’s lastLogTerm is greater → candidate is up-to-date.
        # If terms are equal, the one with the larger lastLogIndex is up-to-date.
        # Equal is fine too (it’s “at least as up-to-date”).
        up_to_date = (candidate_last_term, candidate_last_idx) >= (my_term, my_idx)

        if can_vote and up_to_date:
            self.voted_for = candidate_id
            self.backup()
            
            self._reset_election_timer()
            # done reset timer?
            return (self.current_term, True)
        
        return (self.current_term, False)

    def backup(self):
        self.storage.save(
            self.current_term,
            self.voted_for,
            self.commit_index,
            self.log
        )
    
    async def on_append_entries(
        self,
        term: int,
        leader_id: str,
        prev_idx: int,
        prev_term: int,
        entries: list['LogEntry'],
        leader_commit: int,
        ) -> tuple[int, bool]:
        """
        Appends entries to the Node
        if new entries are passed and
        acts as heartbeat when not entries
        are passed
        """
        
        # Reject stale leader
        if term < self.current_term:
            return (self.current_term, False)
        
        # Update or step down on newer term
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.role = Role.FOLLOWER
            self.backup()
        
        self.leader_id = leader_id
        # Heartbeat of replication behavior.
        self._reset_election_timer()

        my_idx, _my_term = self.last_index_term()
        
        # 1) Consistency check. Happy path, require prev to match
        if prev_idx <  self.snap_last_idx:
            # Leader is trying to append entries that are not in our current log
            ok = True
        elif prev_idx == self.snap_last_idx:
            ok = (prev_term == self.snap_last_term)
        else:
            # This branch is when Leader is referencing our current log
            # instead of an index in the snapshot.
            if prev_idx <= my_idx:
                print(f"[{self.node_id}] Eval if own last term == prev term: {
                    (prev_idx, my_idx, len(self.log), self.base_log_index())
                    }")
            
            if prev_idx >= self.base_log_index():
                arr_pos = prev_idx - self.base_log_index()
                ok = (0 <= arr_pos < len(self.log) and self.log[arr_pos].term == prev_term)
            else:
                print(f"[{self.node_id}] No ok in on_append_entries")
                ok = False # Out of range
        
        if not ok:
            print(f"[{self.node_id}] Rejected Append from {leader_id}. Current term: {self.current_term} Cond? {prev_idx <= my_idx} {(prev_idx, my_idx)}")
            
            return (self.current_term, False)

        # We should insert in the log considering logical indexes
        insert_from = prev_idx + 1
        # Compute array position
        arr_pos = insert_from - self.base_log_index()

        if arr_pos < 0:
            # Insert overlaps snapshot boundary?
            arr_pos = 0
        
        # Truncate entries if node (follower)
        # to match what the leader have as it should be it index
        # Explanation: since prev_idx/prev_term matched, Raft guarantees the history up to prev_idx is identical.
        # We throw away anything after prev_idx and append leader’s entries.
        if self.log and arr_pos < len(self.log):
            del self.log[arr_pos:]
        
        # 2) Append new entries
        for e in entries:
            self.log.append(e)
        
        self.backup()

        # 3) Update commit index to leader's commit if ahead
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.last_log_index())  # or snap_last_idx + len(self.log)
            self._apply_commits()

        return (self.current_term, True)

    def _apply_commits(self):
        base = self.base_log_index()  # snap_last_idx + 1

        while self.last_applied < self.commit_index:
            self.last_applied += 1

            # Anything at or before snapshot tip is already reflected in kv
            if self.last_applied <= self.snap_last_idx:
                continue

            arr_pos = self.last_applied - base
            if not (0 <= arr_pos < len(self.log)):
                raise Exception(
                    f"[{self.node_id}] apply out of range: "
                    f"last_applied={self.last_applied} base={base} len(log)={len(self.log)} "
                    f"snap_last_idx={self.snap_last_idx} commit_index={self.commit_index}"
                )

            e = self.log[arr_pos]
            if e.key is not None:
                print(f"[{self.node_id}] updated key {e.key} to value {e.value} (commit)")
                self.kv[e.key] = e.value

        self.maybe_snapshot()

    
    async def _replicate_to(self, peer: str) -> bool:
        
        while self.role == Role.LEADER:
            ni = self.next_index[peer]

            # --- NEW: follower is behind our snapshot → must InstallSnapshot ---
            if ni <= self.snap_last_idx:
                t, ok = await self.transport.install_snapshot(
                    target=peer,
                    term=self.current_term,
                    leader_id=self.node_id,
                    snap_last_idx=self.snap_last_idx,
                    snap_last_term=self.snap_last_term,
                    kv=dict(self.kv),
                )
                if t > self.current_term:
                    self.step_down(new_term=t)
                    return False
                if ok:
                    self.match_index[peer] = self.snap_last_idx
                    self.next_index[peer] = self.snap_last_idx + 1
                return ok

            prev_idx = ni - 1
            prev_term = self.term_at(prev_idx)   # snapshot-aware

            base = self.base_log_index()
            start = ni - base                    # logical → array
            entries = self.log[start:] if start >= 0 else self.log[:]

            t, ok = await self.transport.append_entries(
                target=peer,
                term=self.current_term,
                leader_id=self.node_id,
                prev_idx=prev_idx,
                prev_term=prev_term,
                entries=entries,
                leader_commit=self.commit_index,
            )

            if t > self.current_term:
                self.step_down(new_term=t)
                return False

            if ok:
                # success means follower now matches our last logical index
                last_idx = self.last_log_index()
                self.match_index[peer] = last_idx
                self.next_index[peer] = last_idx + 1
                await self._advance_commit_index()
                return True

            # back up and retry
            if self.next_index[peer] > 1:
                self.next_index[peer] -= 1
            else:
                return False
    
    async def propose_put(self, key: str, value: str):
        print(f"[{self.node_id}] Proposing {key}:{value}")
        
        if self.role != Role.LEADER:
            #TODO: Handle redirection to leader
            print(f"[{self.node_id}] Leader rejected put command for key {key}")
            return False

        #1)  Append locally
        entry = LogEntry(term=self.current_term, key=key, value=value)
        self.log.append(entry)
        self.backup()

        idx, term = self.last_index_term()

        acks = 1 + sum(1 for was_ok in await asyncio.gather(*(self._replicate_to(p) for p in self.peers)) if was_ok)

        # 3) Commit on majoriy and apply (leader-only rule: entry.term must equal to current_term)
        if acks > (1 + len(self.peers)) // 2 and entry.term == self.current_term:
            self.commit_index = idx
            await self._advance_commit_index()

            #4) Optional but nice, send a heartbeat that carries leader_commit forward
            await asyncio.gather(*(self.transport.append_entries(
                target=p,
                term=self.current_term,
                leader_id = self.node_id,
                prev_idx= idx,
                prev_term=term,
                entries=[],
                leader_commit=self.commit_index
            ) for p in self.peers))

            return True
        else:
            print(f"{[self.node_id]} Not enough ACK received")
        
        return False

    def _quorum_size(self) -> int:
        # Total Number of Nodes in the Cluster. Peers + Node itself.
        return len(self.peers) + 1
    
    async def _advance_commit_index(self):
        if self.role != Role.LEADER:
            return

        # logical last index of leader’s log
        own_last = self.snap_last_idx + len(self.log)

        # match_index values should also be logical
        match_list = list(self.match_index.values()) + [own_last]
        if not match_list:
            return

        s = sorted(match_list, reverse=True)
        majority_pos = (self._quorum_size() // 2)
        if majority_pos >= len(s):
            return

        candidate = s[majority_pos]
        candidate = min(candidate, own_last)  # clamp just in case

        if candidate <= self.commit_index or candidate == 0:
            return

        # If candidate is within snapshot-covered region, it's already "committed"
        if candidate <= self.snap_last_idx:
            self.commit_index = max(self.commit_index, candidate)
            self._apply_commits()
            return

        # Raft rule: only advance commitIndex by majority for entries from current term
        if self.term_at(candidate) == self.current_term:
            self.commit_index = candidate
            self._apply_commits()

    

    def base_log_index(self) -> int:
        """First logical index stored in local storage"""
        return self.snap_last_idx + 1
    
    def last_log_index(self) -> int:
        return self.snap_last_idx + len(self.log)

    def term_at(self, idx: int) -> int:
        if idx == 0:
            return 0
        if idx == self.snap_last_idx:
            return self.snap_last_term
        
        if idx < self.snap_last_idx:
            raise ValueError("term_at below snapshot")
            # or optinally return 0
        
        base = self.base_log_index()  # snap_last_idx + 1
        arr_pos = idx - base
        return self.log[arr_pos].term

    
    def maybe_snapshot(self):
        # Only generate a new Snapshot if we have advanced enough from
        # previous snapshot

        if self.commit_index - self.snap_last_idx < RaftNode.SNAP_INTERVAL:
            return

        # 1) Advance snapshot to the commit tip
        new_snap_idx = self.commit_index
        new_snap_term = self.term_at(new_snap_idx)

        # How many terms to remove from the front
        old_logical_base: int = self.base_log_index()
        cut: int = max(0, min(len(self.log), new_snap_idx - old_logical_base + 1))

        self.snap_last_idx = new_snap_idx
        self.snap_last_term = new_snap_term

        # 2) Persist Snapshot
        self.snapstore.save(
            self.snap_last_idx,
            self.snap_last_term,
            self.kv
        )

        # Example Values
        # Log:      [4] [5] [6] [7] [8]
        #                            ^ not commited
        #                        ^ commited
        #            ^ old_logical_base = 4
        # Snapshot: [0] [1] [2] [3]
        #                        |
        #                        -- snap_last_idx =3

        # Truncate log: remove entries that are now in the snapshot
        # cut was already calculated correctly above using old_logical_base
        if cut > 0:
            self.log = self.log[cut:]
        
        self.backup()
    
    async def on_install_snapshot(
        self,
        term,
        leader_id,
        snap_last_idx,
        snap_last_term,
        kv,
        ):
        if term < self.current_term:
            return (self.current_term, False)

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.role = Role.FOLLOWER
            self.backup()

        # accept snapshot only if it's newer
        if snap_last_idx <= self.snap_last_idx:
            self._reset_election_timer()
            return (self.current_term, True)

        self.leader_id = leader_id  # remember current leader

        # 1) replace state machine
        self.kv = dict(kv)

        # 2) drop log prefix covered by snapshot
        old_base = self.base_log_index()          # old snap_last_idx + 1
        # entries in current log start at old_base
        # keep only entries with logical index > snap_last_idx
        keep_from = max(0, snap_last_idx - old_base + 1)
        self.log = self.log[keep_from:]

        # 3) advance snapshot metadata
        self.snap_last_idx = snap_last_idx
        self.snap_last_term = snap_last_term

        # 4) advance commit/applied pointers
        self.commit_index = max(self.commit_index, snap_last_idx)
        self.last_applied = max(self.last_applied, snap_last_idx)

        # 5) persist snapshot + raft state
        self.snapstore.save(self.snap_last_idx, self.snap_last_term, dict(self.kv))
        self.backup()  # persist term/voted_for/log

        self._reset_election_timer()
        print(f"[{self.node_id}] Snapshot installed from leader_id {leader_id}")
        return (self.current_term, True)

    def __str__(self):
        return f"Node {self.node_id} is {self.role} | LogSize {len(self.log)} in Term {self.current_term}"    
