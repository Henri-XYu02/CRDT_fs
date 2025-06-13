from typing import override

import serde

from sortedcontainers import SortedDict
from merkle_crdt.merkle_crdt import MerkleCRDT

@serde
class FugueNode:
    key: 

class RGACRDT(MerkleCRDT):
    root: FugueNode
    deleted: set

    def __init__(self, path: str, replica: int):
        super().__init__(path, replica)
        self.inner = SortedDict()
        self.deleted = set()


    @override
    def apply_operation(self, op: list[str]):
        if len(op) == 0:
            return
        if len(op) == 1:
            # remove
            pass
        if len(op) == 3:
            # add
            pass
        height = int(op[0])
        replica = int(op[1])

        if (height, replica) > self.won:
            self.value = op[2]

    # TODO: support compaction
    async def write(self, val):
        async with self.lock:
            self.value = val
            self.won = (self.won[0] + 1, self.replica)
            new_node = self.new_node([str(self.won[0]), str(self.won[1]), self.value], {self.tree.root})
            self.put_node(new_node)
            self.tree.root = new_node.hash_value
            self.applied_ops.add(new_node.hash_value)


    def read(self):
        return self.value



