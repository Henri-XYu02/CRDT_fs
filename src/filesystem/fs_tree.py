"""
Implementation of the filesystem tree structure using Merkle-CRDTs.
"""
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
import time

from ..merkle_crdt import MerkleCRDT

@dataclass
class FSNode:
    """Represents a node in the filesystem tree."""
    inode: int
    name: str
    is_directory: bool
    parent_inode: Optional[int] = None
    children: Set[int] = None  # Set of child inode numbers

    def __post_init__(self):
        if self.children is None:
            self.children = set()

class FSTree:
    """
    Filesystem tree implementation using a Merkle-CRDT.
    This represents the structure of the filesystem, storing only the hierarchy
    and inode numbers. Actual file data and metadata are stored separately.
    """
    def __init__(self, replica_id: str):
        self.crdt = MerkleCRDT(replica_id)
        self.nodes: Dict[int, FSNode] = {}
        self.next_inode = 1
        
        # Create root directory
        self._create_root()

    def _create_root(self) -> None:
        """Create the root directory node."""
        root = FSNode(inode=0, name="/", is_directory=True)
        self.nodes[0] = root
        self.next_inode = 1

    def create_node(self, parent_inode: int, name: str, is_directory: bool) -> Optional[int]:
        """
        Create a new filesystem node.
        
        Args:
            parent_inode: Inode number of the parent directory
            name: Name of the new node
            is_directory: Whether the new node is a directory
            
        Returns:
            The new node's inode number, or None if creation failed
        """
        if parent_inode not in self.nodes:
            return None
        
        parent = self.nodes[parent_inode]
        if not parent.is_directory:
            return None
            
        # Check if name already exists in parent
        for child_inode in parent.children:
            if self.nodes[child_inode].name == name:
                return None
                
        # Create new node
        new_inode = self.next_inode
        self.next_inode += 1
        
        node = FSNode(
            inode=new_inode,
            name=name,
            is_directory=is_directory,
            parent_inode=parent_inode
        )
        
        # Add to local state
        self.nodes[new_inode] = node
        parent.children.add(new_inode)
        
        # Create CRDT operation
        self.crdt.add_operation(
            op_type="create",
            path=f"{parent_inode}/{name}",
            payload={
                "inode": new_inode,
                "is_directory": is_directory,
                "parent_inode": parent_inode
            }
        )
        
        return new_inode

    def delete_node(self, inode: int) -> bool:
        """
        Delete a filesystem node.
        
        Args:
            inode: Inode number of the node to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        if inode not in self.nodes or inode == 0:  # Can't delete root
            return False
            
        node = self.nodes[inode]
        
        # Can't delete non-empty directory
        if node.is_directory and node.children:
            return False
            
        # Remove from parent's children
        if node.parent_inode is not None:
            parent = self.nodes[node.parent_inode]
            parent.children.remove(inode)
            
        # Create CRDT operation
        self.crdt.add_operation(
            op_type="delete",
            path=str(inode),
            payload={"parent_inode": node.parent_inode}
        )
        
        # Remove from local state
        del self.nodes[inode]
        return True

    def move_node(self, inode: int, new_parent_inode: int, new_name: Optional[str] = None) -> bool:
        """
        Move a filesystem node to a new parent and/or rename it.
        
        Args:
            inode: Inode number of the node to move
            new_parent_inode: Inode number of the new parent directory
            new_name: New name for the node (optional)
            
        Returns:
            True if move was successful, False otherwise
        """
        if inode not in self.nodes or new_parent_inode not in self.nodes:
            return False
            
        node = self.nodes[inode]
        new_parent = self.nodes[new_parent_inode]
        
        if not new_parent.is_directory:
            return False
            
        # Check if new name already exists in new parent
        if new_name:
            for child_inode in new_parent.children:
                if self.nodes[child_inode].name == new_name:
                    return False
                    
        # Remove from old parent
        if node.parent_inode is not None:
            old_parent = self.nodes[node.parent_inode]
            old_parent.children.remove(inode)
            
        # Update node
        node.parent_inode = new_parent_inode
        if new_name:
            node.name = new_name
            
        # Add to new parent
        new_parent.children.add(inode)
        
        # Create CRDT operation
        self.crdt.add_operation(
            op_type="move",
            path=str(inode),
            payload={
                "new_parent": new_parent_inode,
                "new_name": new_name if new_name else node.name
            }
        )
        
        return True

    def get_node(self, inode: int) -> Optional[FSNode]:
        """Get a node by its inode number."""
        return self.nodes.get(inode)

    def list_directory(self, inode: int) -> List[FSNode]:
        """List the contents of a directory."""
        if inode not in self.nodes:
            return []
            
        node = self.nodes[inode]
        if not node.is_directory:
            return []
            
        return [self.nodes[child_inode] for child_inode in node.children]

    def lookup(self, parent_inode: int, name: str) -> Optional[int]:
        """
        Look up a node by name in a directory.
        
        Args:
            parent_inode: Inode number of the parent directory
            name: Name to look up
            
        Returns:
            Inode number of the found node, or None if not found
        """
        if parent_inode not in self.nodes:
            return None
            
        parent = self.nodes[parent_inode]
        if not parent.is_directory:
            return None
            
        for child_inode in parent.children:
            if self.nodes[child_inode].name == name:
                return child_inode
                
        return None 