"""
Distributed filesystem implementation using Merkle-CRDTs.
This module provides the core filesystem functionality and FUSE interface.
"""

from .fs_tree import FSTree
from .fs_operations import FSOperations
from .metadata_store import MetadataStore
from .data_store import DataStore
from .fuse_interface import FuseInterface

__all__ = ['FSTree', 'FSOperations', 'MetadataStore', 'DataStore', 'FuseInterface'] 