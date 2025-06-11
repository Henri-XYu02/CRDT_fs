import random
from typing import Set, override
import uuid

import pyfuse3
from merkle_crdt.merkle_crdt import MerkleCRDT

ROOT_ID = pyfuse3.ROOT_INODE
TRASH_ID = 3

class MerkleKTree(MerkleCRDT):
    ktree: dict[int, tuple[int, str]]
    # A more useful presentation
    child: dict[int, Set[tuple[str, int]]]
    oplog: list[tuple[tuple[int, int], tuple[int, str] | None, int, str, int]]
    childlogs: dict[int, list[int]] # Maps items to log entries for LWW detection
    # (time, oldparent, oldmeta, parent, meta, child)
    # time: height, replica

    # TODO: serialize
    def __init__(self, path: str, replica: int):
        super().__init__(path, replica)
        self.ktree = {}
        self.oplog = []
        self.child = {}
        self.childlogs = {}
        self.move((0, "root", 1))

    # TODO: async io
    @override
    def apply_operation(self, op: list[str]):
        self.apply_operations([op])

    def ancestor(self, parent, child):
        if parent == child:
            return True
        if parent not in self.child:
            return False
        for c in self.child[parent]:
            if self.ancestor(c, child):
                return True
        return False

    @override
    def apply_operations(self, ops: list[list[str]]):
        if len(ops) == 0:
            return
        # (height, replica, parent, meta, child)
        ops_processed = [((int(i[0]), int(i[1])), int(i[2]), i[3], int(i[4])) for i in reversed(ops) if len(i) != 0]
        # Items are already ordered by timestamp, but we need to find where we start undoing
        # Already locked here
        # Undo items
        visited_parents = set()
        while len(self.oplog) != 0 and self.oplog[-1][0] > ops_processed[-1][0]:
            item = self.oplog.pop()
            if self.childlogs[item[4]] == len(self.oplog):
                self.childlogs[item[4]].pop()
            self.child[item[2]].remove((item[3], item[4]))
            if item[1] is not None:
                self.ktree[item[4]] = item[1]
                self.child[item[1][0]].add((item[1][1], item[4]))
                visited_parents.add(item[1][0])
            else:
                self.ktree.pop(item[4])
            ops_processed.append((item[0], item[2], item[3], item[4]))
        # Sort by time
        ops_processed.sort(key = lambda x: x[0])
        # Redo items
        for i in range(len(ops_processed)):
            v = ops_processed[i]
            if i != 0 and ops_processed[i-1] == v:
                continue
            # Add to oplog by checking old parent
            oldp = self.ktree.get(v[3], None)
            self.oplog.append((v[0], oldp, v[1], v[2], v[3]))
            # Fix crashing
            if v[3] not in self.child:
                self.child[v[3]] = set()
            # Check if operation is invalid, skip modifying state if so
            if self.ancestor(v[3], v[1]):
                continue
            # Only do LWW check for valid operations
            if v[3] not in self.childlogs:
                self.childlogs[v[3]] = []
            self.childlogs[v[3]].append(len(self.oplog))
            # Now modify state
            self.ktree[v[3]] = (v[1], v[2])
            if oldp is not None and (oldp[1], v[3]) in self.child[oldp[0]]:
                self.child[oldp[0]].remove((oldp[1], v[3]))
            if v[1] not in self.child:
                self.child[v[1]] = set()
            self.child[v[1]].add((v[2], v[3]))
            visited_parents.add(v[1])
        # Technically done, but want to check if there's conflicts in filenames
        new_moves: list[tuple[int, str, int]] = []
        for parent in visited_parents:
            if parent == TRASH_ID:
                continue
            metas = {}
            interesting_metas = set()
            for child in self.child.get(parent, []):
                if child[0] in metas:
                    metas[child[0]].add(child[1])
                    interesting_metas.add(child[0])
                else:
                    metas[child[0]] = {child[1]}
            for meta in interesting_metas:
                # Rename conflicting based on LWW
                children = list(sorted(metas[meta], key= lambda x: self.oplog[self.childlogs[x][-1]][0]))
                for child in children[1:]:
                    last_op = self.oplog[self.childlogs[child][-1]]
                    i = 0
                    s = f"{meta}_{last_op[0][1]}_{i}"
                    while s in meta:
                        i += 1
                        s = f"{meta}_{last_op[0][1]}_{i}"

                    new_moves.append((last_op[2], s, child))
        for move in new_moves:
            self.move(move)
        print(self.ktree, self.oplog, self.child, self.childlogs)

    def move(self, op: tuple[int, str, int]):
        root = self.tree.nodes[self.tree.root]
        new_op = [str(root.height + 1), str(self.replica), str(op[0]), op[1], str(op[2])]
        self.apply_operation(new_op)
        new_node = self.new_node(new_op, {self.tree.root})
        self.put_node(new_node)
        self.tree.root = new_node.hash_value


    # OPERATIONS VISIBLE TO THE WORLD
    async def remove(self, id: int):
        async with self.lock:
            rand = uuid.uuid1().int>>64
            self.move((TRASH_ID, str(rand), id))


    async def mkdir(self, parent: int, name: str) -> int:
        async with self.lock:
            rand = (uuid.uuid1().int>>64 & ~(1 << 63)) | (0 << 64)
            self.move((parent, name, rand))
            return rand

    async def mkf(self, parent: int, name: str) -> int:
        async with self.lock:
            rand = (uuid.uuid1().int>>64 & ~(1 << 63)) | (1 << 63)
            self.move((parent, name, rand))
            return rand

    async def rename(self, id: int, npar: int, nname: str):
        async with self.lock:
            self.move((npar, nname, id))


    # How the KTree handles conflicts
    # Same name: One filename gets the file, the other gets renamed

    # TODO: implement special case handling for directories
    # Same name occupied by file and directory: Directory gets the filename, file gets renamed
    # Same dirname: Move operations are generated to merge the directories

    async def write(self, val):
        async with self.lock:
            self.value = val
            self.won = (self.won[0] + 1, self.replica)
            new_node = self.new_node([str(self.won[0]), str(self.won[1]), self.value], {self.tree.root})
            self.put_node(new_node)
            self.tree.root = new_node.hash_value


    def read(self):
        return self.value


