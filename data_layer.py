from __future__ import annotations
import os
import asyncio
import json
import aiofiles
from hash_ring import HashRing
from network_layer import Node, Node_Table
import socket
from itertools import islice


class Data:
    def __init__(self, id, save_hash, title, path, check_hash=0):
        self.id = id
        self.save_hash = save_hash
        self.title = title
        self.path = path
        self.check_hash = check_hash
        self.save_path = "./storage/" + self.title
        try:
            self.file_size = os.path.getsize(self.path)
        except FileNotFoundError:
            print(f"File '{self.path}' not found.")

    # !!!!!
    def __eq__(self, other:Data):
        if isinstance(other, Data):
            return self.title == other.title
        return False

    async def send_data(self, dest_ip, dest_port=8888, request=b'SEND_DATA\n\n', timeout=100):
        """
            用于将本Data对象发送至指定节点
            :param request: 发送该数据包的请求：SEND_DATA or REPLY_DOWNLOAD
            :param dest_ip: 目的ip
            :param dest_port: 目的端口，可使用缺省值8888
            :param timeout： 超时上限，缺省值100s

            """
        while True:
            try:
                reader, writer = await asyncio.open_connection(dest_ip, dest_port)

                ## 1 Prepare non-binary data/准备非二进制数据
                data = {
                    "id": self.id,
                    "save_hash": self.save_hash,
                    "title": self.title,
                    "path": self.path,
                    "check_hash": self.check_hash,
                    "file_size": self.file_size}
                json_data = json.dumps(data).encode('utf-8')
                ## 2 read file
                # currently, it's reading from local address
                with open(self.save_path, 'rb') as file:
                    pdf_data = file.read()

                ## 3 send data，收集ACK，没有收到的加入到queue中隔一段时间继续发送
                writer.write(request)
                writer.write(json_data)
                writer.write(b'\n\n')  # 使用两个换行符作为分隔符
                writer.write(pdf_data)
                await writer.drain()
                # Wait for ACK with a timeout
                ack = await asyncio.wait_for(reader.readuntil(b'ACK\n\n'), timeout=timeout)

                break

            except asyncio.TimeoutError:
                print("Timeout occurred when sending data. Retry in 10 seconds")
                await asyncio.sleep(10)
                print("Retrying...")
            except OSError as e:
                print(f"Connection failed when sending data. Error: {e}. Retry in 10 seconds")
                await asyncio.sleep(10)
                print("Retrying...")

            finally:
                if 'writer' in locals():
                    writer.close()
                    await writer.wait_closed()

    # function: using Consistent Hashing Algorithm to decide whether to save
    def need_to_save(self, ring: HashRing, node_id: str):
        # return True  # for debug
        # 等确定方案再放出来 ↓

        nodes = ring.get_nodes_for_key(self.title)
        # 映射到哈希环使用的两个节点
        for node in nodes:
            if node_id == node.id:
                return True
            # print(f"Key '{self.path}' maps to node: {node}")
        return False

    # currently assume no transmission error
    def verify_file_with_check_hash(self):
        return True


class DataTable:
    def __init__(self, initial_datas=None):
        if initial_datas is None:
            self.datas = []
        else:
            self.datas = initial_datas

    def __iter__(self):
        return iter(self.datas)

    def add_data(self, data):
        self.datas.append(data)

    def remove_node(self, data):
        self.datas.remove(data)

    def contains(self, data):
        return data in self.datas

    def save_table(self, filename='data_table.json'):
        with open(filename, 'w') as file:
            json.dump(self.datas, file)

    def load_table(self, filename='data_table.json'):
        try:
            with open(filename, 'r') as file:
                self.datas = json.load(file)
        except FileNotFoundError:
            print(f"File '{filename}' not found. Initializing with an empty table.")

    async def request_data_table(self, dest_ip, dest_port):
        reader, writer = await asyncio.open_connection(dest_ip, dest_port)
        try:
            # 发送请求
            writer.write(b'REQUEST_DATA_TABLE\n\n')
            await writer.drain()

            # 接收数据
            data = await reader.readuntil(b'\n\n')
            json_data_table = json.loads(data.decode('utf-8'))
            data_dict_list = json_data_table
            # 将数据字典转换回Data对象
            self.datas = [
                Data(id=data_dict['id'], save_hash=data_dict['save_hash'], title=data_dict['title'],
                     path=data_dict['path'], check_hash=data_dict['check_hash'])
                for data_dict in data_dict_list
            ]

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error during request_data_table: {e}")

        writer.close()
        await writer.wait_closed()
