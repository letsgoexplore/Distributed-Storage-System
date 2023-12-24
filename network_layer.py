# import package
from __future__ import annotations
import asyncio
import json

# TODO
# Data structure
class Node:
    def __init__(self, id, ip, hash):
        for key, value in locals().items():
            if key != "self":
                setattr(self, key, value)
    ## AT: lyh
    def __eq__(self, other):
        if isinstance(other, Node):
            return self.id == other.id
        return False

# TODO
class Node_Table:
    def __init__(self, initial_nodes = None):
        if initial_nodes is None:
            self.nodes = []
        else:
            self.nodes = initial_nodes

    def add_node(self, node):
        self.nodes.append(node)

    def remove_node(self, node):
        self.nodes.remove(node)

    def contains(self, node):
        return node in self.nodes
    
    ## AT: lyh
    async def send_table(ip, port):
        pass