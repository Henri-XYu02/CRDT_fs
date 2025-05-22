"""
Merkle-CRDT implementation for distributed filesystem.
This module provides the core Merkle-CRDT functionality that combines CRDTs with Merkle trees
for causal consistency and efficient synchronization.
"""

from .merkle_tree import MerkleTree
from .merkle_node import MerkleNode
from .merkle_crdt import MerkleCRDT

__all__ = ['MerkleTree', 'MerkleNode', 'MerkleCRDT'] 