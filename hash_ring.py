"""
    hash_ring
    ~~~~~~~~~~~~~~
    Implements consistent hashing that can be used when
    the number of server nodes can increase or decrease (like in memcached).

    Consistent hashing is a scheme that provides a hash table functionality
    in a way that the adding or removing of one slot
    does not significantly change the mapping of keys to slots.

    More information about consistent hashing can be read in these articles:

        "Web Caching with Consistent Hashing":
            http://www8.org/w8-papers/2a-webserver/caching/paper2.html

        "Consistent hashing and random trees:
        Distributed caching protocols for relieving hot spots on the World Wide Web (1997)":
            http://citeseerx.ist.psu.edu/legacymapper?did=38148


    Example of usage::

        memcache_servers = ['192.168.0.246:11212',
                            '192.168.0.247:11212',
                            '192.168.0.249:11212']

        ring = HashRing(memcache_servers)
        server = ring.get_node('my_key')

    :copyright: 2008 by Amir Salihefendic.
    :license: BSD
"""

import math
import sys
import bisect
import hashlib
import copy

if sys.version_info >= (2, 5):
    import hashlib
    md5_constructor = hashlib.md5
else:
    import md5
    md5_constructor = md5.new



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

    def to_dict(self):
        """将 Hash 对象转换为可以序列化的字典"""
        return {
            'id': self.id,
            'hash': self.hash
        }
    
    def from_dict(data):
        """从字典转换回 Hash 对象"""
        return Hash(data['id'])
    
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


class HashRing(object):
    def __init__(self):
        self.sorted_hashes = []
        self.nodes = []
    
    def clone(self):
        """clone function"""
        new_ring = HashRing()
        new_ring.sorted_hashes = copy.deepcopy(self.sorted_hashes)
        new_ring.nodes = copy.deepcopy(self.nodes)
        return new_ring

    def add_node(self, node):
        """adding node to hashring"""
        hash_obj = Hash(node.id)  # using id to generate Hash object
        bisect.insort(self.sorted_hashes, hash_obj)
        self.nodes.append(node)  # using hash as the key

    def remove_node(self, node_id):
        """remove a node from"""
        hash_obj = Hash(node_id)
        if hash_obj in self.sorted_hashes:
            self.sorted_hashes.remove(hash_obj)
            for node in self.nodes:
                if node.id == node_id:
                    self.nodes.remove(node)
        # if hash_obj.hash in self.nodes:
        #     self.sorted_hashes.remove(hash_obj)
        #     del self.nodes[hash_obj.hash]

    def get_nodes_for_key(self, key):
        """根据key获取哈希环上顺时针方向的后两个节点"""
        hash_key_obj = Hash(key)  # 创建一个Hash对象而不是直接使用整数哈希值
        index = bisect.bisect_right(self.sorted_hashes, hash_key_obj) % len(self.sorted_hashes)
        next_nodes_hash = [self.sorted_hashes[(index + i) % len(self.sorted_hashes)] for i in range(2)]
        next_nodes = []
        for node in self.nodes:
            if Hash(node.id) in next_nodes_hash:
                next_nodes.append(node)
        return next_nodes

    def add_node_and_list_change(self, data_list, new_node):
        """计算添加新节点后哈希环上的变化"""
        changes = []

        # 计算添加节点前的情况
        before_addition = {item.title: self.get_nodes_for_key(item.title) for item in data_list}

        # 添加新节点
        self.add_node(new_node)

        # 计算添加节点后的情况并比较
        for item in data_list:
            after_addition = self.get_nodes_for_key(item.title)
            if before_addition[item.title] != after_addition:
                changes.append((item, before_addition[item.title], after_addition))

        return changes
    
    def remove_node_and_list_change(self, data_list, remove_node_id):
        """计算删除节点后哈希环上的变化"""
        changes = []

        # 计算删除节点前的情况
        before_removal = {item.title: self.get_nodes_for_key(item.title) for item in data_list}

        # 删除节点
        self.remove_node(remove_node_id)

        # 计算删除节点后的情况并比较
        for item in data_list:
            after_removal = self.get_nodes_for_key(item.title)
            if before_removal[item.title] != after_removal:
                changes.append((item, before_removal[item.title], after_removal))

        return changes