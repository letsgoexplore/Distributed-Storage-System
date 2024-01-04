import json
from hash_ring import HashRing, Hash
from bisect import bisect

class Node:
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port

    def to_dict(self):
        return {"id": self.id, "ip": self.ip, "port": self.port}

    @staticmethod
    def from_dict(data):
        return Node(data["id"], data["ip"], data["port"])

class NodeTable(HashRing):
    def __init__(self):
        super().__init__()

    def encode(self):
        data = {
            "sorted_hashes": self.sorted_hashes,
            "nodes": {hash_val: node.to_dict() for hash_val, node in self.nodes.items()}
        }
        return json.dumps(data).encode('utf-8')

    @staticmethod
    def decode(encoded_data):
        data = json.loads(encoded_data.decode('utf-8'))
        node_table = NodeTable()
        node_table.sorted_hashes = data["sorted_hashes"]
        node_table.nodes = {hash_val: Node.from_dict(node_data) for hash_val, node_data in data["nodes"].items()}
        return node_table
    
    # ... 其他方法 ...

# 使用 NodeTable 的示例
node_table = NodeTable()
node = Node("haha", "1.1.1.1", 8888)
node_table.add_node(node)
encoded_node_table = node_table.encode()
decoded_node_table = NodeTable.decode(encoded_node_table)
print(encoded_node_table)
print(decoded_node_table)
