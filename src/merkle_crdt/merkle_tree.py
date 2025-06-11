"""
Implementation of a Merkle tree data structure for efficient state synchronization.
"""
from typing import Any, Dict, List, Optional, Set
import hashlib
from .merkle_node import MerkleNode

class MerkleTree:
    """
    A Merkle tree implementation that maintains a hash tree of CRDT operations.
    This allows for efficient detection of differences between replicas.
    """
    def __init__(self):
        self.root = MerkleNode()
        self.leaves: Dict[str, Any] = {}  # Hash -> Data mapping

    def add_leaf(self, data_hash: str, data: Any) -> None:
        """
        Add a new leaf to the Merkle tree.
        
        Args:
            data_hash: Hash of the data to store
            data: The actual data to store
        """
        self.leaves[data_hash] = data
        self._update_tree()

    def _update_tree(self) -> None:
        """
        Rebuild the Merkle tree structure from the current set of leaves.
        This is called whenever leaves are added or removed.
        """
        # Sort leaves for deterministic tree construction
        sorted_hashes = sorted(self.leaves.keys())
        
        # Create leaf nodes
        current_level = [
            MerkleNode(hash_value=h, is_leaf=True)
            for h in sorted_hashes
        ]
        
        # Build tree levels until we reach the root
        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), 2):
                left = current_level[i]
                right = current_level[i + 1] if i + 1 < len(current_level) else None
                
                parent = MerkleNode()
                parent.left = left
                parent.right = right
                
                # Compute parent hash
                if right:
                    combined = left.hash_value + right.hash_value
                else:
                    combined = left.hash_value
                parent.hash_value = hashlib.sha256(combined.encode()).hexdigest()
                
                next_level.append(parent)
            
            current_level = next_level
        
        self.root = current_level[0] if current_level else MerkleNode()

    def get_root_hash(self) -> str:
        """Get the root hash of the Merkle tree."""
        return self.root.hash_value

    def diff(self, other_tree: 'MerkleTree') -> List[str]:
        """
        Find the differences between this tree and another tree.
        
        Args:
            other_tree: Another Merkle tree to compare against
            
        Returns:
            List of leaf hashes that differ between the trees
        """
        if self.get_root_hash() == other_tree.get_root_hash():
            return []

        different_leaves: Set[str] = set()
        self._find_differences(self.root, other_tree.root, different_leaves)
        return list(different_leaves)

    def _find_differences(
        self,
        node1: Optional[MerkleNode],
        node2: Optional[MerkleNode],
        differences: Set[str]
    ) -> None:
        """
        Recursively find differences between two Merkle trees.
        
        Args:
            node1: Node from first tree
            node2: Node from second tree
            differences: Set to collect differing leaf hashes
        """
        if not node1 or not node2:
            # One side is missing, add all leaves from the other side
            if node1:
                self._collect_leaves(node1, differences)
            if node2:
                self._collect_leaves(node2, differences)
            return

        if node1.hash_value == node2.hash_value:
            return

        if node1.is_leaf and node2.is_leaf:
            differences.add(node1.hash_value)
            return

        self._find_differences(node1.left, node2.left, differences)
        self._find_differences(node1.right, node2.right, differences)

    def _collect_leaves(self, node: Optional[MerkleNode], leaves: Set[str]) -> None:
        """
        Collect all leaf hashes under a node.
        
        Args:
            node: The node to collect leaves from
            leaves: Set to collect leaf hashes into
        """
        if not node:
            return
        
        if node.is_leaf:
            leaves.add(node.hash_value)
            return

        self._collect_leaves(node.left, leaves)
        self._collect_leaves(node.right, leaves) 