# import package
from __future__ import annotations
import asyncio
import json

## AT: lyh
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


## AT: pr
class Data:
    def __init__(self, id, num_hash, title, file, check_hash, locations):
        # here, 'file' refer to the local address of the file
        #       'locations' refers to which place the file is saved
        for key, value in locals().items():
            if key != "self":
                setattr(self, key, value)
    
    def __eq__(self, other):
        if isinstance(other, Data):
            return self.id == other.id
        return False
    
    async def send_data(ip, port):
        reader, writer = await asyncio.open_connection('127.0.0.1', 8888)

        ## 1 Prepare non-binary data/准备非二进制数据
        data = {
            "number": 123,
            "hash": "somehashvalue",
            "title": "Example Title"
        }
        json_data = json.dumps(data).encode('utf-8')

        ## 2 read file
        # currently, it's reading from local address
        with open('your_file.pdf', 'rb') as file:
            pdf_data = file.read()
        
        ## 3 send data，收集ACK，没有收到的加入到queue中隔一段时间继续发送（使用循环）
        writer.write(json_data)
        writer.write(b'\n\n')  # 使用两个换行符作为分隔符
        writer.write(pdf_data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()


    # 功能： 根据收到的数据，创建Data数据
    async def receive_data() -> Data:
        return Data()
    
    # function: using Consistent Hashing Algorithm to decide whether to save
    def save_or_not(node_id):
        pass
    
    # currently assume no transmission error
    def verify_file_with_check_hash():
        return True

## AT: pr
class Data_Table:
    def __init__(self, initial_datas = None):
        if initial_datas is None:
            self.datas = []
        else:
            self.datas = initial_datas
    
    def add_data(self, data):
        self.datas.append(data)

    def remove_node(self, data):
        self.datas.remove(data)

    def contains(self, data):
        return data in self.data
    
    # 将数据传给需要的节点
    def handle_join_request(node_id):
        pass
    

    

        
        





# 如何进行相应
# 需要多线程？因为它需要同时处理相互之间的消息同时要

# Network Layer

if __name__ == "__main__":
    # part 1: 维护变量
    pass
    # part 2: 进行初始化

    # part 3: 开启循环监听