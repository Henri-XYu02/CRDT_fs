"""
Implementation of a node in the Merkle tree.
"""
from typing import Optional
from dataclasses import dataclass

@dataclass
class MerkleNode:
    """
    A node in the Merkle tree.
    Each node contains a hash value and optional left and right children.
    Leaf nodes contain actual data hashes, while internal nodes contain hashes of their children.
    """
    hash_value: str = ""
    is_leaf: bool = False
    left: Optional['MerkleNode'] = None
    right: Optional['MerkleNode'] = None 