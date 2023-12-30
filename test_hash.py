import hashlib
import bisect

class Node:
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port

class Hash:
    def __init__(self, id):
        self.id = id
        self.hash = self.gen_key(id)

    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.
        md5 is currently used because it mixes well.
        """
        b_key = self._hash_digest(key)
        return self._hash_val(b_key, lambda x: x)

    def _hash_val(self, b_key, entry_fn):
        """Generates a hash value from a byte key."""
        return ((b_key[entry_fn(3)] << 24)
                | (b_key[entry_fn(2)] << 16)
                | (b_key[entry_fn(1)] << 8)
                |  b_key[entry_fn(0)])

    def _hash_digest(self, key):
        """Generates a byte digest for a key using MD5."""
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        return list(map(int, m.digest()))

    def __eq__(self, other):
        return self.hash == other.hash

    def __lt__(self, other):
        return self.hash < other.hash

    def __gt__(self, other):
        return self.hash > other.hash

class HashRing:
    def __init__(self):
        self.sorted_hashes = []
        self.nodes = {}

    def add_node(self, node):
        """adding node to hashring"""
        hash_obj = Hash(node.id)  # using id to generate Hash object
        bisect.insort(self.sorted_hashes, hash_obj)
        self.nodes[hash_obj.hash] = node  # using hash as the key

    def remove_node(self, node_id):
        """remove a node from"""
        hash_obj = Hash(node_id)
        if hash_obj.hash in self.nodes:
            self.sorted_hashes.remove(hash_obj)
            del self.nodes[hash_obj.hash]

    def get_nodes_for_key(self, key):
        """根据key获取哈希环上顺时针方向的后两个节点"""
        hash_key_obj = Hash(key)  # 创建一个Hash对象而不是直接使用整数哈希值
        index = bisect.bisect_right(self.sorted_hashes, hash_key_obj) % len(self.sorted_hashes)
        next_nodes = [self.sorted_hashes[(index + i) % len(self.sorted_hashes)] for i in range(2)]
        return [self.nodes[node.hash] for node in next_nodes]
    
    def print_all_nodes(self):
        """打印所有节点的信息"""
        for node in self.nodes.values():
            print(f"ID: {node.id}, IP: {node.ip}, Port: {node.port}")

    def add_node_and_list_change(self, data_list, new_node):
        """计算添加新节点后哈希环上的变化"""
        changes = []

        # 计算添加节点前的情况
        before_addition = {item.id: self.get_nodes_for_key(item.id) for item in data_list}

        # 添加新节点
        self.add_node(new_node)

        # 计算添加节点后的情况并比较
        for item in data_list:
            after_addition = self.get_nodes_for_key(item.id)
            if before_addition[item.id] != after_addition:
                changes.append((item, before_addition[item.id], after_addition))

        return changes
    
    def remove_node_and_list_change(self, data_list, remove_node_id):
        """计算删除节点后哈希环上的变化"""
        changes = []

        # 计算删除节点前的情况
        before_removal = {item.id: self.get_nodes_for_key(item.id) for item in data_list}

        # 删除节点
        self.remove_node(remove_node_id)

        # 计算删除节点后的情况并比较
        for item in data_list:
            after_removal = self.get_nodes_for_key(item.id)
            if before_removal[item.id] != after_removal:
                changes.append((item, before_removal[item.id], after_removal))

        return changes

hash_ring = HashRing()
# 添加一些节点...
hash_ring.add_node(Node("node1", "192.168.1.1", 8080))
hash_ring.add_node(Node("node2", "192.168.1.2", 8080))

# 打印所有节点
hash_ring.print_all_nodes()

# 计算添加新节点后的变化
print("----------- start adding -------------")
data_list = [Node("node3", "192.168.1.3", 8080), Node("node4", "192.168.1.4", 8080)]
new_node = Node("node5", "192.168.1.5", 8080)
changes = hash_ring.add_node_and_list_change(data_list, new_node)
for change in changes:
    item, nodes_before, nodes_after = change
    print(f"Item: {item.id}")
    print("Before:", [(node.id, node.ip, node.port) for node in nodes_before])
    print("After:", [(node.id, node.ip, node.port) for node in nodes_after])
hash_ring.print_all_nodes()

# 删除部分节点
print("----------- start deleting -------------")
data_list = [Node("node3", "192.168.1.3", 8080), Node("node4", "192.168.1.4", 8080)]
remove_node_id = "node2"  # 假设我们要删除的节点ID
changes = hash_ring.remove_node_and_list_change(data_list, remove_node_id)
for change in changes:
    item, nodes_before, nodes_after = change
    print(f"Item: {item.id}")
    print("Before:", [(node.id, node.ip, node.port) for node in nodes_before])
    print("After:", [(node.id, node.ip, node.port) for node in nodes_after])
hash_ring.print_all_nodes()