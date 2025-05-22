"""
Core implementation of the Merkle-CRDT that combines CRDT operations with a Merkle tree structure.
"""
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
import hashlib
import time

from .merkle_tree import MerkleTree
from .merkle_node import MerkleNode

@dataclass
class Operation:
    """Represents a CRDT operation with its associated metadata."""
    timestamp: float
    replica_id: str
    operation_type: str
    path: str
    payload: Any
    dependencies: Set[str]  # Set of operation hashes this operation depends on

    def compute_hash(self) -> str:
        """Compute a cryptographic hash of the operation."""
        content = f"{self.timestamp}:{self.replica_id}:{self.operation_type}:{self.path}:{self.payload}:{sorted(self.dependencies)}"
        return hashlib.sha256(content.encode()).hexdigest()

class MerkleCRDT:
    """
    A CRDT implementation that uses a Merkle tree to track operations and their causal relationships.
    This provides efficient synchronization and conflict resolution between replicas.
    """
    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.merkle_tree = MerkleTree()
        self.operations: Dict[str, Operation] = {}  # Hash -> Operation
        self.applied_ops: Set[str] = set()  # Set of applied operation hashes

    def add_operation(self, op_type: str, path: str, payload: Any, dependencies: Optional[Set[str]] = None) -> str:
        """
        Add a new operation to the CRDT.
        
        Args:
            op_type: Type of operation (e.g., 'create', 'delete', 'update')
            path: Filesystem path the operation applies to
            payload: Operation-specific data
            dependencies: Set of operation hashes this operation depends on
            
        Returns:
            Hash of the new operation
        """
        op = Operation(
            timestamp=time.time(),
            replica_id=self.replica_id,
            operation_type=op_type,
            path=path,
            payload=payload,
            dependencies=dependencies or set()
        )
        
        op_hash = op.compute_hash()
        self.operations[op_hash] = op
        self.merkle_tree.add_leaf(op_hash, op)
        
        return op_hash

    def get_missing_operations(self, other_merkle_tree: MerkleTree) -> List[str]:
        """
        Compare with another replica's Merkle tree to find missing operations.
        
        Args:
            other_merkle_tree: Merkle tree from another replica
            
        Returns:
            List of operation hashes that are missing from this replica
        """
        return self.merkle_tree.diff(other_merkle_tree)

    def apply_operation(self, op_hash: str) -> bool:
        """
        Apply an operation if all its dependencies have been applied.
        
        Args:
            op_hash: Hash of the operation to apply
            
        Returns:
            True if operation was applied, False if dependencies are missing
        """
        if op_hash in self.applied_ops:
            return True

        op = self.operations.get(op_hash)
        if not op:
            return False

        # Check if all dependencies are met
        if not all(dep in self.applied_ops for dep in op.dependencies):
            return False

        # Apply the operation (actual implementation would depend on operation type)
        self.applied_ops.add(op_hash)
        return True

    def get_state(self) -> Tuple[Dict[str, Operation], str]:
        """
        Get the current state of the CRDT and the root hash of the Merkle tree.
        
        Returns:
            Tuple of (operations dict, root hash)
        """
        return self.operations, self.merkle_tree.get_root_hash() 