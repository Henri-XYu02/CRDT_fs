from typing import override
from merkle_crdt.merkle_crdt import MerkleCRDT


class MerkleLWWRegister(MerkleCRDT):
    value: str
    won: tuple[int, int]

    def __init__(self, path: str, replica: int):
        super().__init__(path, replica)
        self.won = (0, 0)
        self.value = ""


    @override
    def apply_operation(self, op: list[str]):
        if len(op) == 0:
            return
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


    def read(self):
        return self.value



