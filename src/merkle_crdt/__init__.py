"""
Merkle-CRDT implementation for distributed filesystem.
This module provides the core Merkle-CRDT functionality that combines CRDTs with Merkle trees
for causal consistency and efficient synchronization.
"""

from .merkle_crdt import MerkleCRDT
from .merkle_lww import MerkleLWWRegister
#from .merkle_fugue import MerkleFugueString
#from .merkle_ktree import MerkleKTree

__all__ = ['MerkleCRDT', 'MerkleLWWRegister', 'MerkleFugueString', 'MerkleKTree'] 
