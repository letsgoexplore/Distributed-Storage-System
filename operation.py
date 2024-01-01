from __future__ import annotations
import shutil
import asyncio
import json
import os
import aiofiles
import socket

from hash_ring import HashRing
from network_layer import Node, NodeTable
from data_layer import Data, DataTable, StorageServer

from Config import ROOT_IP, ROOT_PORT

class RespondingServer:
    def __init__(self, data_table: DataTable, node_id: str, node_table: NodeTable, ip: str='127.0.0.1', port: int=8888):
        self.node_id = node_id
        self.ip = ip
        # self.ip = socket.gethostbyname(socket.gethostname())
        self.port = port
        self.node = Node(id=self.node_id, ip=self.ip, port=self.port)
        self.state = True

        self.node_table = node_table
        self.data_table = data_table
    
    async def handle_client(self, reader, writer):
        while self.state:
            try:
                request = await reader.readuntil(b'\n\n')
                # 收到获取datatable请求
                if request == b'REQUEST_DATA_TABLE\n\n':
                    print("Client requested Data_Table")
                    await self.handle_request_data_table(reader, writer)

                elif request == b'REQUEST_DATA\n\n':
                    await self.handle_request_data(reader, writer)

                # 收到数据
                elif request == b'SEND_DATA\n\n':
                    await self.handle_send_data(reader, writer)

                elif request == b'JOIN\n\n':
                    await self.handle_join_network(reader, writer)

                elif request == b'QUIT\n\n':
                    await self.handle_quit_network(reader, writer)

                elif request == b'UPDATE_NET\n\n':
                    await self.handle_update_network(reader, writer)

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

    async def run_server(self):
        try:
            server = await asyncio.start_server(self.handle_client, '127.0.0.1', self.port)
            addr = server.sockets[0].getsockname()
            print(f'DataServer running on {addr}')
            async with server:
                await server.serve_forever()

        except Exception as e:
            print(f"Error during run DataServer: {e}")
    
    def setup():
        # 从json文件加载节点表
        Node_Table(initial_nodes=None, new_start=False)
    
    def start_network(self):
        # 构建初始化节点表，节点表被保存在json文件
        self.node_table.add_node(self.node)

    async def send_message(self, dest_ip, dest_port, request, data, timeout=10):
        while True:
            try:
                reader, writer = await asyncio.open_connection(dest_ip, dest_port)
                writer.write(request)
                writer.write(data.encode('utf-8'))
                writer.write(b'\n\n')  # Using two newline characters as a separator

                response = await asyncio.wait_for(reader.readuntil(b'\n\n'), timeout)
                if response == b'ACK':
                    print(f"Received ACK from {dest_ip}")
                    break

            except asyncio.TimeoutError:
                print(f"Timeout occurred when connecting to {dest_ip} (quit).")
            except OSError as e:
                print(f"Connection failed when connecting to {dest_ip} (quit). Error: {e}.")
                await asyncio.sleep(10)
                print("Retrying...")
            finally:
                if 'writer' in locals():
                    writer.close()
                    await writer.wait_closed()

    async def join_network(self):

        # step 1: ask root node for join request
        data = self.node_id + "/" + self.ip + "/" + self.port
        join_request = b'JOIN\n\n'
        await self.send_message(ROOT_IP, ROOT_PORT, join_request, data)

        # step 2: request node table from root node
        await self.request_node_table(ROOT_IP, ROOT_PORT)

        # step 3: broadcast to all for join request
        tasks = [self.send_message(node.ip, node.port, join_request, data) for node in self.node_table.nodes if node.ip != self.ip and node.ip != ROOT_IP]
        await asyncio.gather(*tasks)

        # setp 4: update all data
        await self.request_personal_data_from_table()

    async def handle_join_network(self, reader, writer):
        
        data = await reader.readuntil(b'\n\n')
        data = data[:-2]

        data = data.split("/")
        node_id = data[0]
        ip = data[1]
        port = data[2]
        node = Node(node_id, ip, port)
        self.node_table.add_node_and_list_change(self.data_table,node)

        writer.write(b'ACK\n\n')

    async def quit_network(self):
        self.node_id + "/" + self.ip + "/" + self.port
        request = b'QUIT\n\n'
        # 向目标服务器发送请求

        async def connect_to_node(node):
            nonlocal ip, dest_port, request, data

            try:
                reader, writer = await asyncio.open_connection(node.ip, dest_port)
                writer.write(request)
                writer.write(data.encode('utf-8'))
                writer.write(b'\n\n')  # Using two newline characters as a separator

            except asyncio.TimeoutError:
                print(f"Timeout occurred when connecting to {node.ip} (quit).")
            except OSError as e:
                print(f"Connection failed when connecting to {node.ip} (quit). Error: {e}.")
                await asyncio.sleep(10)
                print("Retrying...")

            finally:
                if 'writer' in locals():
                    writer.close()
                    await writer.wait_closed()
        # 相当于遍历nodetable的一个循环
        # TODO
        # 这里遍历有问题，得仔细看一下怎么广播
        await asyncio.gather(*(connect_to_node(node) for node in node_table.nodes if node.ip != ip))
        await asyncio.sleep(1)     # 等待其他节点的node_table更新完成，理论上应该不需要
        need_to_send_list = []
        for data in data_table:
            if data.need_to_save(ring, self_node):
                need_to_send_list.append(data)
        if len(need_to_send_list) > 0:
            node_table.remove_node(ip)
            # new_ring = HashRing(node_table)
            ring.remove_node(self_node)
            # title是被hash的内容，给出存储的两个节点
            node1, node2 = ring.get_nodes_for_key(need_to_send_list[0].title)
            for data in need_to_send_list:
                await data.send_data(node1.ip, dest_port=dest_port)
                await data.send_data(node2.ip, dest_port=dest_port)

    async def handle_quit_network(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        data = data[:-2]

        ip = data
        node_table = Node_Table(new_start=False)
        node_table.remove_node(ip)

        # 如何初始化↓ TODO
        node = Node(ip)
        self.ring.remove_node(node.id)
        # new_ring = HashRing(node_table)
        # self.ring = new_ring

    async def request_node_table(self, dest_ip, dest_port):
        request = b'REQUEST_NODE_TABLE\n\n'

        while True:
            try:
                reader, writer = await asyncio.open_connection(dest_ip, dest_port)
                writer.write(request)


            except asyncio.TimeoutError:
                print(f"Timeout occurred when connecting to {dest_ip} (quit).")
            except OSError as e:
                print(f"Connection failed when connecting to {dest_ip} (quit). Error: {e}.")
                await asyncio.sleep(10)
                print("Retrying...")
            finally:
                if 'writer' in locals():
                    writer.close()
                    await writer.wait_closed()

    
    # TODO
    async def handle_request_node_table(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        data = data[:-2]

        self.node_table.decode(data)
        writer.write()

        
    # 只是个示例, 之后还要改，就是在store_data时，一定要先把他放到save_path中（默认: ./storage/）
    async def store_data(id, title, path):
        data = Data(id=id, save_hash=0, title=title, path=path)
        save_path = data.save_path
        try:
            shutil.copy(path, save_path)

        except FileNotFoundError:
            print(f"Error: File {path} not found")
        except PermissionError:
            print(f"Error: Unable to copy file to {save_path}, permission denied")
        except Exception as e:
            print(f"An unknown error occurred: {e}")

        # 再执行后续操作
        pass
    
    async def handle_store_data(self, reader, writer):
        """when new data comes, this function is used"""
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

    async def read_data():
        pass

    async def request_data(self, data:Data, ip):
        request = b'REQUEST_DATA\n\n'
        try:
            reader, writer = await asyncio.open_connection(ip, 8888)
            data0 = {
                "id": data.id,
                "save_hash": data.save_hash,
                "title": data.title,
                "path": data.path,
                "check_hash": data.check_hash,
                "file_size": data.file_size}
            json_data = json.dumps(data0).encode('utf-8')
            writer.write(request)
            writer.write(json_data)
            writer.write(b'\n\n')  # 使用两个换行符作为分隔符
            await writer.drain()
            pdf_data = await reader.readexactly(data.file_size)
            save_path = data.save_path
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            async with aiofiles.open(save_path, 'wb') as file:
                await file.write(pdf_data)

        except asyncio.TimeoutError:
            print("Timeout occurred when download_from_remote.")
        except OSError as e:
            print(f"Connection failed when download_from_remote. Error: {e}.")
            await asyncio.sleep(10)
            print("Retrying...")

        finally:
            if 'writer' in locals():
                writer.close()
                await writer.wait_closed()
    
    async def handle_request_data(self, writer, reader):
        """send back the requested file"""
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

    # TODO
    async def request_data_table():
        pass

    async def handle_request_data_table(self, reader, writer):
        """send back data table without exact file"""
        data_dict_list = [
            {'id': data.id, 'save_hash': data.save_hash, 'title': data.title, 'path': data.path,
             'check_hash': data.check_hash, 'file_size': data.file_size}
            for data in self.data_table.datas
        ]
        json_data = json.dumps(data_dict_list)
        writer.write(json_data.encode('utf-8'))
        writer.write(b'\n\n')  # Using two newline characters as a separator

    async def request_personal_data_from_table(self):
        for data in self.data_table:
            if data.need_to_save(self.node_table, self.node_id):
                nodes = self.node_table.get_nodes_for_key(data.title)
                await self.request_data(data, nodes[0].ip, nodes[0].port)

    

    

    

    async def handle_update_network(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        data = data[:-2]

        json_dir = json.loads(data)
        node_table = Node_Table(initial_nodes=None)
        node_table.update(nodes=json_dir["IPs_nodes"], next_nodes=json_dir["IPs_next_nodes"],
                          pre_nodes=json_dir["IPs_pre_nodes"])
        # 如何给接入节点ring的结构是个问题↓ TODO
        ring = HashRing(node_table)
        await asyncio.sleep(1)  # 等待其他节点的node_table更新完成，理论上应该不需要
        for data in self.data_table:
            if data.need_to_save(ring, self.node_id):
                nodes = self.ring.get_nodes_for_key(data.title)
                for node in nodes:
                    if node.id != node.id:
                        await self.request_data(data, node.ip)

    async def handle_quit_network(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        data = data[:-2]

        ip = data
        node_table = Node_Table(new_start=False)
        node_table.remove_node(ip)

        # 如何初始化↓ TODO
        node = Node(ip)
        self.ring.remove_node(node.id)
        # new_ring = HashRing(node_table)
        # self.ring = new_ring

    # 对应operation中的decode_message()


            

async def download_from_remote(data: Data, dest_ip, dest_port, timeout=1000):
    request = b'DOWNLOAD\n\n'
    try:
        reader, writer = await asyncio.open_connection(dest_ip, dest_port)
        data0 = {
            "id": data.id,
            "save_hash": data.save_hash,
            "title": data.title,
            "path": data.path,
            "check_hash": data.check_hash,
            "file_size": data.file_size}
        json_data = json.dumps(data0).encode('utf-8')
        writer.write(request)
        writer.write(json_data)
        writer.write(b'\n\n')  # 使用两个换行符作为分隔符
        await writer.drain()
        pdf_data = await reader.readexactly(data.file_size)
        download_path = './download/' + data.title
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        async with aiofiles.open(download_path, 'wb') as file:
            await file.write(pdf_data)

    except asyncio.TimeoutError:
        print("Timeout occurred when download_from_remote.")
    except OSError as e:
        print(f"Connection failed when download_from_remote. Error: {e}.")
        await asyncio.sleep(10)
        print("Retrying...")

    finally:
        if 'writer' in locals():
            writer.close()
            await writer.wait_closed()



# async def start_service():
#     # step 0: initiate
#     setup()
#     port_data_service = 8888
#     data_table = DataTable()

#     # 更多运行示例可见test_data_layer.py
#     storage_server = StorageServer(data_table=data_table, node_id='node1', ring=HashRing(), ip='127.0.0.1',
#                                 port=port_data_service)

#     # Create tasks for running servers, 这里还可以添加其他异步任务，如将来要执行的命令行（感觉可以）
#     tasks = [
#         asyncio.create_task(storage_server.run())
#     ]

#     # Run all tasks concurrently
#     await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(start_service())
