# import package
from __future__ import annotations
import asyncio
import json


# 具体使用方法：当节点创建的时候，将new_start作为True，并且输入一个初始节点(ip与parent ip相同)，节点表将会存储在json文件中
# 之后每次使用节点表都建立Node_Table(initial_nodes = None ,new_start = False),该对象会自动从JSON文件加载节点链表
# ，然后每次执行操作如果对链表有所修改都会记录在json文件上


# 每个节点保存自己的ip地址和自己的父节点ip地址，用父节点ip地址代替自己的id，
# 这样可以避免重复冲突，请求加入时也可以直接告诉所有节点自己要加在谁的后面即可，无需询问所有id
# Data structure
class Node:
    def __init__(self, parent_ip, ip):
        '''for key, value in locals().items():
            if key != "self":
                setattr(self, key, value)'''

        self.ip = ip
        self.parent_ip = parent_ip

    def __eq__(self, other):
        if isinstance(other, Node):
            return self.parent_ip == other.parent_ip
        return False


# TODO
class Node_Table:
    def __init__(self, initial_nodes=None, new_start=True):
        self.file_name1 = "./IPs_nodes.json"
        self.file_name2 = "./IPs_next_nodes.json"
        self.file_name3 = "./IPs_pre_nodes.json"

        # 如果是全新的节点表
        if new_start:

            # 该字典维护每一个ip访问自己的节点
            self.nodes = {}
            # 该字典维护每一个ip访问该ip下一个节点
            self.next_nodes = {}
            self.pre_nodes = {}
            if initial_nodes != None:
                self.nodes[initial_nodes.ip] = initial_nodes.ip
                self.pre_nodes[initial_nodes.ip] = initial_nodes.parent_ip
                self.next_nodes[initial_nodes.parent_ip] = initial_nodes.ip

            # 维护一个json文件，里面包括所有的IP地址与circle上的连接关系
            print(self.file_name1)

            with open(self.file_name1, "w") as f:
                json.dump(self.nodes, f)
            with open(self.file_name2, "w") as f:
                json.dump(self.next_nodes, f)
            with open(self.file_name3, "w") as f:
                json.dump(self.pre_nodes, f)
        # 从json文件加载
        else:
            with open(self.file_name1, 'r') as f:
                self.nodes = json.load(f)
            with open(self.file_name2, 'r') as f:
                self.next_nodes = json.load(f)
            with open(self.file_name3, 'r') as f:
                self.pre_nodes = json.load(f)

    def add_node(self, node):
        self.nodes[node.ip] = node.ip
        pre_children = self.next_nodes[node.parent_ip]
        self.pre_nodes[pre_children] = node.ip
        self.next_nodes[node.parent_ip] = node.ip
        self.next_nodes[node.ip] = pre_children

        with open(self.file_name1, "w") as f:
            json.dump(self.nodes, f)
        with open(self.file_name2, "w") as f:
            json.dump(self.next_nodes, f)
        with open(self.file_name3, "w") as f:
            json.dump(self.pre_nodes, f)

    def remove_node(self, ip):

        children = self.nodes[ip]
        parent_ip = self.pre_nodes[ip]
        self.next_nodes[parent_ip] = children
        self.pre_nodes[children] = parent_ip
        del (self.nodes[ip])
        del (self.next_nodes[ip])
        del (self.pre_nodes[ip])

        with open(self.file_name1, "w") as f:
            json.dump(self.nodes, f)
        with open(self.file_name2, "w") as f:
            json.dump(self.next_nodes, f)
        with open(self.file_name3, "w") as f:
            json.dump(self.pre_nodes, f)

    def update(self, nodes, next_nodes, pre_nodes):
        self.nodes = nodes
        self.next_nodes = next_nodes
        self.pre_nodes = pre_nodes
        with open(self.file_name1, "w") as f:
            json.dump(self.nodes, f)
        with open(self.file_name2, "w") as f:
            json.dump(self.next_nodes, f)
        with open(self.file_name3, "w") as f:
            json.dump(self.pre_nodes, f)

    # 返回指定ip的下一个节点
    def next_ip(self, parent_ip):
        return self.next_nodes[parent_ip]

    # 返回指定ip的父亲节点
    def parent_ip(self, ip):
        return self.pre_nodes[ip]

    # 返回节点表里的所有节点
    def all_ip(self):
        return self.nodes.values()

    # 返回节点表里除了own_ip外的所有节点
    def ip_except(self, own_ip):
        ips = []
        for ip in self.nodes.values():
            if ip == own_ip:
                continue
            ips.append(ip)

        return ips

    # 似乎暂时没必要实现了
    # 最后要return node_table，给上层进行encode
    # async def send_table(ip, port):
    #     pass
    # return node_table
