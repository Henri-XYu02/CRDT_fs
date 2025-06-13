import base64
from typing import override
from merkle_crdt.merkle_crdt import MerkleCRDT


class MerkleLWWRegister(MerkleCRDT):
    value: bytes
    won: tuple[int, int]
    dirty: bool

    def __init__(self, path: str, replica: int):
        super().__init__(path, replica)
        self.won = (0, 0)
        self.value = bytes()
        self.dirty = False


    @override
    def apply_operation(self, op: list[str]):
        if len(op) == 0:
            return
        height = int(op[0])
        replica = int(op[1])

        if (height, replica) > self.won:
            self.value = base64.b64decode(op[2])
            self.won = (height, replica)

    # TODO: support compaction
    async def write(self, val: bytes):
        async with self.lock:
            self.dirty = True
            self.value = val


    def _cut_root(self):
        if self.dirty:
            self.won = (self.won[0] + 1, self.replica)
            new_node = self.new_node([str(self.won[0]), str(self.won[1]), base64.b64encode(self.value).decode()], {self.tree.root})
            self.put_node(new_node)
            self.tree.root = new_node.hash_value
            self.applied_ops.add(new_node.hash_value)


    def read(self):
        return self.value



