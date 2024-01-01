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
    
    def to_dict(self):
        """将 Node 对象转换为可以序列化的字典"""
        return {
            'id': self.id,
            'ip': self.ip,
            'port': str(self.port)
        }

    def from_dict(data):
        """从字典转换回 Node 对象"""
        return Node(data['id'], data['ip'], int(data['port']))

class NodeTable(HashRing):
    def __init__(self):
        super().__init__()
    
    def encode(self):
        """turning NodeTable into utf-8"""
        data = {
            "sorted_hashes": [h.to_dict() for h in self.sorted_hashes],
            "nodes": [n.to_dict() for n in self.nodes]
        }
        return json.dumps(data).encode('utf-8')
    
    def decode(self, encoded_data):
        data = json.loads(encoded_data.decode('utf-8'))
        self.sorted_hashes = [Hash.from_dict(h) for h in data["sorted_hashes"]]
        self.nodes = [Node.from_dict(n) for n in data["nodes"]]
    
    async def request_data_table(self):
        pass

    async def handle_request_node_table(self):
        pass


