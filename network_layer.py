# import package
from __future__ import annotations
from hash_ring import Hash, HashRing
import asyncio
import json


class Node:
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port
        

class NodeTable(HashRing):
    def __init__(self):
        super().__init__()
    
    def encode(self):
        """turning NodeTable into utf-8"""
        data = {
            "sorted_hashes": self.sorted_hashes,
            "nodes": self.nodes
        }
        return json.dumps(data).encode('utf-8')
    
    def decode(encoded_data):
        data = json.loads(encoded_data.decode('utf-8'))
        node_table = NodeTable()
        node_table.sorted_hashes = data["sorted_hashes"]
        node_table.nodes = data["nodes"]
        return node_table
    
    async def request_data_table(self):
        pass

    async def handle_request_node_table(self):
        pass


