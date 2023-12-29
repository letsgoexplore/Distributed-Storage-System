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
                ack = await asyncio.wait_for(reader.readuntil(b'ACK\n'), timeout=timeout)

                if ack == b'ACK\n':
                    # print("Data sent successfully")
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
        return True  # for debug
        # 等确定方案再放出来 ↓

        # node_generator = ring.iterate_nodes(self.title)
        # # 映射到哈希环使用的两个节点
        # for node in islice(node_generator, 2):
        #     if node_id == node:
        #         return True
        #     # print(f"Key '{self.path}' maps to node: {node}")
        # return False

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


class StorageServer:
    def __init__(self, data_table: DataTable, node_id: str, ring: HashRing, ip: str='127.0.0.1', port: int=8888):
        self.data_table = data_table
        self.node_id = node_id
        self.ring = ring
        self.ip = ip
        self.port = port

    async def handle_request_data_table(self, reader, writer):
        data_dict_list = [
            {'id': data.id, 'save_hash': data.save_hash, 'title': data.title, 'path': data.path,
             'check_hash': data.check_hash, 'file_size': data.file_size}
            for data in self.data_table.datas
        ]
        json_data = json.dumps(data_dict_list)
        writer.write(json_data.encode('utf-8'))
        writer.write(b'\n\n')  # Using two newline characters as a separator

    async def handle_send_data(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        json_data = json.loads(data.decode('utf-8'))
        pdf_data = await reader.readexactly(json_data['file_size'])
        received_data = Data(
            id=json_data['id'],
            save_hash=json_data['save_hash'],
            title=json_data['title'],
            path=json_data['path'],
            check_hash=json_data['check_hash'])
        if received_data.need_to_save(self.ring, self.node_id):
            os.makedirs(os.path.dirname(received_data.save_path), exist_ok=True)
            async with aiofiles.open(received_data.save_path, 'wb') as file:
                await file.write(pdf_data)
        if received_data not in self.data_table:
            self.data_table.add_data(received_data)

        writer.write(b'ACK\n')

    async def handle_download(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        json_data = json.loads(data.decode('utf-8'))
        received_data = Data(
            id=json_data['id'],
            save_hash=json_data['save_hash'],
            title=json_data['title'],
            path=json_data['path'],
            check_hash=json_data['check_hash'])

        # 认为向你发起download的请求，则已知你拥有这个文件，还是判断一下，但应该没问题（如果达成前面的共识的话）
        if received_data in self.data_table:
            with open(received_data.save_path, 'rb') as file:
                pdf_data = file.read()
            writer.write(pdf_data)

    async def handle_join_network(self, writer, reader):
        data = await reader.readuntil(b'\n\n')
        data = data[:-2]

        data = data.split("/")
        ip = data[0]
        parent_ip = data[1]

        node = Node(ip=ip, parent_ip=parent_ip)
        node_table = Node_Table(new_start=False)
        node_table.add_node(node)

        # 获取本机ip地址
        # ip = "192.168.52.1"
        self_ip = socket.gethostbyname(socket.gethostname())

        # 并且如果自己是父节点，向请求添加的节点发送节点链表
        if self_ip == parent_ip:
            json_dir = {}
            json_dir["IPs_nodes"] = node_table.nodes
            json_dir["IPs_next_nodes"] = node_table.next_nodes
            json_dir["IPs_pre_nodes"] = node_table.pre_nodes
            json_data = json.dumps(json_dir)
            request = b"UPDATE_NET\n\n"
            # send message，将该json_str(字符串)发送给请求加入的节点，让他更新node table

            writer.write(json_data.encode('utf-8'))
            writer.write(b'\n\n')  # Using two newline characters as a separator

    async def handle_update_network(self, writer, reader, data):
        data = await reader.readuntil(b'\n\n')
        data = data[:-2]

        json_dir = json.loads(data)
        node_table = Node_Table(initial_nodes=None)
        node_table.update(nodes=json_dir["IPs_nodes"], next_nodes=json_dir["IPs_next_nodes"],
                          pre_nodes=json_dir["IPs_pre_nodes"])

    async def handle_quit_network(self, writer, reader, data):
        data = await reader.readuntil(b'\n\n')
        data = data[:-2]

        ip = data
        node_table = Node_Table(new_start=False)
        node_table.remove_node(ip)

    # 对应operation中的decode_message()
    async def handle_client(self, reader, writer):
        try:
            request = await reader.readuntil(b'\n\n')
            # 收到获取datatable请求
            if request == b'REQUEST_DATA_TABLE\n\n':
                print("Client requested Data_Table")
                await self.handle_request_data_table(reader, writer)

            # 收到数据
            elif request == b'SEND_DATA\n\n':
                await self.handle_send_data(reader, writer)

            elif request == b'JOIN\n\n':
                pass

            elif request == b'QUIT\n\n':
                pass

            elif request == b'UPDATE_NET\n\n':
                pass

            elif request == b'DOWNLOAD\n\n':
                await self.handle_download(reader, writer)
            else:
                print("Unknown request from the client")
            await writer.drain()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error during handle_client: {e}")

        writer.close()

    async def run(self):
        try:
            server = await asyncio.start_server(self.handle_client, '127.0.0.1', self.port)
            addr = server.sockets[0].getsockname()
            print(f'DataServer running on {addr}')
            async with server:
                await server.serve_forever()

        except Exception as e:
            print(f"Error during run DataServer: {e}")
