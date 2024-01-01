from __future__ import annotations
import shutil
import asyncio
import json
import os
import aiofiles
import socket
import sys

from hash_ring import HashRing
from network_layer import Node, NodeTable
from data_layer import Data, DataTable

from Config import ROOT_IP, ROOT_PORT

class StorageServer:
    def __init__(self, data_table: DataTable, node_id: str, node_table: NodeTable, ip: str='127.0.0.1', port: int=ROOT_PORT):
        self.node_id = node_id
        self.ip = ip
        # self.ip = socket.gethostbyname(socket.gethostname())
        self.port = port
        self.node = Node(id=self.node_id, ip=self.ip, port=self.port)
        self.state = True

        self.node_table = node_table
        self.data_table = data_table
    
    async def handle_client(self, reader, writer):
    # while self.state:
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
                await self.handle_store_data(reader, writer)

            elif request == b'REQUEST_NODE_TABLE\n\n':
                await self.handle_request_node_table(reader, writer)

            elif request == b'JOIN\n\n':
                await self.handle_join_network(reader, writer)

            elif request == b'QUIT\n\n':
                await self.handle_quit_network(reader, writer)

            elif request == b'UPDATE_NET\n\n':
                await self.handle_update_network(reader, writer)

            elif request == b'DOWNLOAD\n\n':
                await self.handle_download(reader, writer)

            elif request == b'UPLOAD\n\n':
                await self.handle_upload(reader, writer)

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
            server = await asyncio.start_server(self.handle_client, self.ip, self.port)
            addr = server.sockets[0].getsockname()
            print(f'DataServer running on {addr}')
            async with server:
                await server.serve_forever()

        except Exception as e:
            print(f"Error during run DataServer: {e}")
    
    # def setup():
    #     # 从json文件加载节点表
    #     Node_Table(initial_nodes=None, new_start=False)
    
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
                if response == b'ACK\n\n':
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
        data = self.node_id + "/" + self.ip + "/" + str(self.port)
        join_request = b'JOIN\n\n'
        await self.send_message(ROOT_IP, ROOT_PORT, join_request, data)

        # step 2: request node table from root node
        await self.request_node_table(ROOT_IP, ROOT_PORT)
        await self.request_data_table(ROOT_IP, ROOT_PORT)

        # step 3: broadcast to all for join request
        tasks = [self.send_message(node.ip, node.port, join_request, data) for node in self.node_table.nodes if node.ip != self.ip and node.ip != ROOT_IP]
        await asyncio.gather(*tasks)

        # setp 4: update all data
        await self.request_personal_data_from_table()

    async def handle_join_network(self, reader, writer):
        
        data = await reader.readuntil(b'\n\n')
        data = data.decode('utf-8')
        data = data[:-2]

        data = data.split("/")
        node_id = data[0]
        ip = data[1]
        port = data[2]
        node = Node(node_id, ip, port)
        self.node_table.add_node_and_list_change(self.data_table,node)

        writer.write(b'ACK\n\n')

    async def quit_network(self):
        data = self.node_id + "/" + self.ip + "/" + str(self.port)
        request = b'QUIT\n\n'
        await self.send_message(ROOT_IP, ROOT_PORT, request, data)

    async def handle_quit_network(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        data = data.decode('utf-8')
        data = data[:-2]

        ip = data
        node_table = NodeTable(new_start=False)
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
                await writer.drain()

                data = await reader.readuntil(b'\n\n')
                self.node_table.decode(data)
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

    # TODO
    async def handle_request_node_table(self, reader, writer):
        encode_table = self.node_table.encode()
        writer.write(encode_table)
        writer.write(b'\n\n')
        await writer.drain()
        
    # 只是个示例, 之后还要改，就是在store_data时，一定要先把他放到save_path中（默认: ./storage/）
    # async def store_data(id, title, path):
    #     data = Data(id=id, save_hash=0, title=title, path=path)
    #     save_path = data.save_path
    #     try:
    #         shutil.copy(path, save_path)

    #     except FileNotFoundError:
    #         print(f"Error: File {path} not found")
    #     except PermissionError:
    #         print(f"Error: Unable to copy file to {save_path}, permission denied")
    #     except Exception as e:
    #         print(f"An unknown error occurred: {e}")

    #     # 再执行后续操作
    #     pass
    
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
            check_hash=json_data['check_hash'],
            file_size=json_data['file_size'])
        if received_data.need_to_save(self.node_table, self.node_id):
            os.makedirs(os.path.dirname(received_data.save_path), exist_ok=True)
            async with aiofiles.open(received_data.save_path, 'wb') as file:
                await file.write(pdf_data)
        if received_data not in self.data_table:
            self.data_table.add_data(received_data)

        writer.write(b'ACK\n\n')

    # async def read_data():
    #     pass

    async def request_data(self, data:Data, ip):
        request = b'REQUEST_DATA\n\n'
        while True:
            try:
                reader, writer = await asyncio.open_connection(ip, ROOT_PORT)
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
                break

            except asyncio.TimeoutError:
                print("Timeout occurred when download_from_remote.")
                await asyncio.sleep(10)
                print("Retrying...")
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
            check_hash=json_data['check_hash'],
            file_size=json_data['file_size'])

        # 认为向你发起download的请求，则已知你拥有这个文件，还是判断一下，但应该没问题（如果达成前面的共识的话）
        if received_data in self.data_table:
            with open(received_data.save_path, 'rb') as file:
                pdf_data = file.read()
            writer.write(pdf_data)

    # TODO
    async def request_data_table(self, dest_ip, dest_port):
        while True:
            try:
                reader, writer = await asyncio.open_connection(dest_ip, dest_port)
                # 发送请求
                writer.write(b'REQUEST_DATA_TABLE\n\n')
                await writer.drain()

                # 接收数据
                data = await reader.readuntil(b'\n\n')
                json_data_table = json.loads(data.decode('utf-8'))
                data_dict_list = json_data_table
                # 将数据字典转换回Data对象
                self.data_table.datas = [
                    Data(id=data_dict['id'], save_hash=data_dict['save_hash'], title=data_dict['title'],
                         path=data_dict['path'], check_hash=data_dict['check_hash'],
                         file_size=data_dict['file_size'])
                    for data_dict in data_dict_list
                ]
                break

            except Exception as e:
                print(f"Error during request_data_table: {e}")
                await asyncio.sleep(10)
                print("Retrying...")

            finally:
                writer.close()
                await writer.wait_closed()

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

    async def handle_download(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        json_data = json.loads(data.decode('utf-8'))
        received_data = Data(
            id=json_data['id'],
            save_hash=json_data['save_hash'],
            title=json_data['title'],
            path=json_data['path'],
            check_hash=json_data['check_hash'],
            file_size=json_data['file_size'])

        if received_data in self.data_table:
            if data.need_to_save(self.node_table, self.node_id):
                with open(received_data.save_path, 'rb') as file:
                    pdf_data = file.read()
                writer.write(pdf_data)
            else:
                node1, node2 = self.node_table.get_nodes_for_key(data.title)
                request = b'REQUEST_DATA\n\n'
                while True:
                    try:
                        r, w = await asyncio.open_connection(node1.ip, ROOT_PORT)
                        data0 = {
                            "id": data.id,
                            "save_hash": data.save_hash,
                            "title": data.title,
                            "path": data.path,
                            "check_hash": data.check_hash,
                            "file_size": data.file_size}
                        json_data = json.dumps(data0).encode('utf-8')
                        w.write(request)
                        w.write(json_data)
                        w.write(b'\n\n')  # 使用两个换行符作为分隔符
                        await w.drain()
                        pdf_data = await r.readexactly(data.file_size)
                        writer.write(pdf_data)
                        break

                    except asyncio.TimeoutError:
                        print("Timeout occurred when download_from_remote.")
                        await asyncio.sleep(10)
                        print("Retrying...")
                    except OSError as e:
                        print(f"Connection failed when download_from_remote. Error: {e}.")
                        await asyncio.sleep(10)
                        print("Retrying...")

                    finally:
                        if 'w' in locals():
                            w.close()
                            await w.wait_closed()

    async def handle_upload(self, reader, writer):
        data = await reader.readuntil(b'\n\n')
        json_data = json.loads(data.decode('utf-8'))
        pdf_data = await reader.readexactly(json_data['file_size'])
        received_data = Data(
            id=len(self.data_table.datas),
            save_hash=json_data['save_hash'],
            title=json_data['title'],
            path=json_data['path'],
            check_hash=json_data['check_hash'],
            file_size=json_data['file_size'])
        if received_data not in self.data_table:
            self.data_table.add_data(received_data)
            nodes = self.node_table.get_nodes_for_key(received_data.title)
            for node in nodes:
                if node.id == self.node_id:
                    os.makedirs(os.path.dirname(received_data.save_path), exist_ok=True)
                    async with aiofiles.open(received_data.save_path, 'wb') as file:
                        await file.write(pdf_data)
                else:
                    await data.send_data(node.ip, dest_port=ROOT_PORT)
            await writer.write(b"SAVE_SUCCESS\n\n")
        else:
            await writer.write(b"SAVE_FAIL\n\n")

# async def download_from_remote(data: Data, dest_ip, dest_port, timeout=1000):
#     request = b'DOWNLOAD\n\n'
#     while True:
#         try:
#             reader, writer = await asyncio.open_connection(dest_ip, dest_port)
#             data0 = {
#                 "id": data.id,
#                 "save_hash": data.save_hash,
#                 "title": data.title,
#                 "path": data.path,
#                 "check_hash": data.check_hash,
#                 "file_size": data.file_size}
#             json_data = json.dumps(data0).encode('utf-8')
#             writer.write(request)
#             writer.write(json_data)
#             writer.write(b'\n\n')  # 使用两个换行符作为分隔符
#             await writer.drain()
#             pdf_data = await reader.readexactly(data.file_size)
#             download_path = './download/' + data.title
#             os.makedirs(os.path.dirname(download_path), exist_ok=True)
#             async with aiofiles.open(download_path, 'wb') as file:
#                 await file.write(pdf_data)

#         except asyncio.TimeoutError:
#             print("Timeout occurred when download_from_remote.")
#         except OSError as e:
#             print(f"Connection failed when download_from_remote. Error: {e}.")
#             await asyncio.sleep(10)
#             print("Retrying...")

#         finally:
#             if 'writer' in locals():
#                 writer.close()
#                 await writer.wait_closed()

async def start_root_node():
    my_server = StorageServer(DataTable(), "root", NodeTable(), ROOT_IP, int(ROOT_PORT))
    my_server.start_network()
    await my_server.run_server()

async def start_node(id, ip, port):
    my_server = StorageServer(DataTable(), id, NodeTable(), ip, int(port))
    await my_server.join_network()
    await my_server.run_server() 

if __name__ == "__main__":
    asyncio.run(start_root_node())
    # if len(sys.argv) > 1:
    #     command = sys.argv[1]
    #     if command == "start_root_node":
    #         asyncio.run(start_root_node())
    #     elif command == "start_node" and len(sys.argv) == 5:
    #         _, id, ip, port = sys.argv[1:]
    #         asyncio.run(start_node(id, ip, port))
    #     else:
    #         print("Invalid arguments")
    # else:
    #     print("No command provided")

# async def start_service():
#     # step 0: initiate
#     setup()
#     port_data_service = ROOT_PORT
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


# if __name__ == "__main__":
#     asyncio.run(start_service())
