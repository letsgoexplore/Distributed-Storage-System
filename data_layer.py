from __future__ import annotations
# import package
import os
import asyncio
import json
import aiofiles
from hash_ring import HashRing
from itertools import islice


async def process_received_data(node, data_receive_server: DataReceiveServer, data_table: DataTableService):
    """
    异步处理接收到的数据的任务函数，在主程序中调用
    :param node: 当前运行的节点的id值或node对象（暂未定）
    :param data_receive_server: 运行中的DataReceiveServer对象
    :param data_table: DataTableService

    """
    print('process_received_data running!')
    while True:
        received_data = await data_receive_server.received_data_queue.get()
        # 维护数据表
        data_table.add_data(received_data)
        if received_data.need_to_save(node):
            pass
        else:
            # 不需要保存的数据进行删除
            try:
                os.remove(received_data.path)
                # 调试点
                # print(f"File '{received_data.path}' deleted successfully.")
            except OSError as e:
                print(f"Error deleting file '{received_data.path}': {e}")


class Data:
    def __init__(self, id, save_hash, title, path, check_hash=0):
        self.id = id
        self.save_hash = save_hash
        self.title = title
        self.path = path
        self.check_hash = check_hash
        try:
            self.file_size = os.path.getsize(self.path)
        except FileNotFoundError:
            print(f"File '{self.path}' not found.")

    # !!!!!
    def __eq__(self, other:Data):
        if isinstance(other, Data):
            return self.path == other.path
        return False

    async def send_data(self, ip, port=8888, request=b'SEND_DATA\n\n', timeout=100):
        """
            用于将本Data对象发送至指定节点
            :param request: 发送该数据包的请求：SEND_DATA or REPLY_DOWNLOAD
            :param ip: 目的ip
            :param port: 目的端口，可使用缺省值8888
            :param timeout： 超时上限，缺省值100s

            """
        while True:
            try:
                reader, writer = await asyncio.open_connection(ip, port)

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
                with open(self.path, 'rb') as file:
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
        node_generator = ring.iterate_nodes(self.title)
        # 映射到哈希环使用的两个节点
        for node in islice(node_generator, 2):
            if node_id == node:
                return True
            # print(f"Key '{self.path}' maps to node: {node}")
        return False

    # currently assume no transmission error
    def verify_file_with_check_hash(self):
        return True


class DataReceiveServer:
    def __init__(self, port=8888):
        self.port = port
        self.received_data_queue = asyncio.Queue()

    async def handle_receive(self, reader, writer):
        # Read JSON data
        data = await reader.readuntil(b'\n\n')
        json_data = json.loads(data.decode('utf-8'))
        pdf_data = await reader.readexactly(json_data['file_size'])
        received_data = Data(
            id=json_data['id'],
            save_hash=json_data['save_hash'],
            title=json_data['title'],
            path=json_data['path'],
            check_hash=json_data['check_hash'])
        os.makedirs(os.path.dirname(json_data['path']), exist_ok=True)
        async with aiofiles.open(json_data['path'], 'wb') as file:
            await file.write(pdf_data)
        await self.received_data_queue.put(received_data)

        # 发送ACK
        writer.write(b'ACK\n')
        await writer.drain()
        writer.close()

    async def run(self):
        server = await asyncio.start_server(self.handle_receive, '127.0.0.1', self.port)
        addr = server.sockets[0].getsockname()
        print(f'Receiving on {addr}')
        async with server:
            await server.serve_forever()


class DataTable:
    def __init__(self, initial_datas=None):
        if initial_datas is None:
            self.datas = []
        else:
            self.datas = initial_datas

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


class DataTableService:
    def __init__(self, initial_datas=None, port=8889):
        if initial_datas is None:
            self.datas = []
        else:
            self.datas = initial_datas
        self.port = port

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

    # 从其他节点请求data_table数据
    async def request_data_table(self, ip, port):
        reader, writer = await asyncio.open_connection(ip, port)
        try:
            # 发送请求
            writer.write(b'REQUEST_DATA_TABLE\n')
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

    async def handle_client(self, reader, writer):
        try:
            request = await reader.readuntil(b'\n')
            # 收到获取datatable请求
            if request == b'REQUEST_DATA_TABLE\n':
                print("Client requested Data_Table")
                data_dict_list = [
                    {'id': data.id, 'save_hash': data.save_hash, 'title': data.title, 'path': data.path,
                     'check_hash': data.check_hash, 'file_size': data.file_size}
                    for data in self.datas
                ]
                json_data = json.dumps(data_dict_list)
                writer.write(json_data.encode('utf-8'))
                writer.write(b'\n\n')  # Using two newline characters as a separator
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
            print(f'Sending DataTable on {addr}')
            async with server:
                await server.serve_forever()

        except Exception as e:
            print(f"Error during run DataTableSendServer: {e}")


class DataServer:
    def __init__(self, data_table: DataTable, node_id: str, ring: HashRing(), ip: str='127.0.0.1', port: int=8888):
        self.data_table = data_table
        self.node_id = node_id
        self.ring = ring
        self.ip = ip
        self.port = port

    # 对应operation中的decode_message()
    async def handle_client(self, reader, writer):
        try:
            request = await reader.readuntil(b'\n\n')
            # 收到获取datatable请求
            if request == b'REQUEST_DATA_TABLE\n\n':
                print("Client requested Data_Table")
                data_dict_list = [
                    {'id': data.id, 'save_hash': data.save_hash, 'title': data.title, 'path': data.path,
                     'check_hash': data.check_hash, 'file_size': data.file_size}
                    for data in self.data_table.datas
                ]
                json_data = json.dumps(data_dict_list)
                writer.write(json_data.encode('utf-8'))
                writer.write(b'\n\n')  # Using two newline characters as a separator

            # 收到数据
            elif request == b'SEND_DATA\n\n':
                data = await reader.readuntil(b'\n\n')
                json_data = json.loads(data.decode('utf-8'))
                pdf_data = await reader.readexactly(json_data['file_size'])
                received_data = Data(
                    id=json_data['id'],
                    save_hash=json_data['save_hash'],
                    title=json_data['title'],
                    path=json_data['path'],
                    check_hash=json_data['check_hash'])
                os.makedirs(os.path.dirname(json_data['path']), exist_ok=True)
                if received_data.need_to_save(self.ring, self.node_id):
                    async with aiofiles.open(json_data['path'], 'wb') as file:
                        await file.write(pdf_data)
                self.data_table.add_data(received_data)
                # await self.received_data_queue.put(received_data)

                writer.write(b'ACK\n')

            # 收到下载数据的回复
            elif request == b'REPLY_DOWNLOAD\n\n':
                data = await reader.readuntil(b'\n\n')
                json_data = json.loads(data.decode('utf-8'))
                pdf_data = await reader.readexactly(json_data['file_size'])
                # 只需要保存文件，不需要再实例化一个Data对象
                # received_data = Data(
                #     id=json_data['id'],
                #     save_hash=json_data['save_hash'],
                #     title=json_data['title'],
                #     path=json_data['path'],
                #     check_hash=json_data['check_hash'])
                download_dir = './download' + json_data['title']
                os.makedirs(os.path.dirname(download_dir), exist_ok=True)
                async with aiofiles.open(download_dir, 'wb') as file:
                    await file.write(pdf_data)
                # await self.received_data_queue.put(received_data)

                writer.write(b'ACK\n')

            elif request == b'JOIN\n\n':
                pass

            elif request == b'QUIT\n\n':
                pass

            elif request == b'DOWNLOAD\n\n':
                pass

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
