from data_layer import Data
from network_layer import Node, NodeTable
from data_layer import Data, DataTable
from Config import ROOT_IP, ROOT_PORT
import asyncio
import json
import os
import aiofiles


class Client:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.node_table = NodeTable()
        self.data_table = DataTable()
        self.cur_id = len(self.data_table.datas)

    def change_server(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port

    async def request_node_table(self):
        reader, writer = await asyncio.open_connection(self.server_ip, self.server_port)
        try:
            # 发送请求
            writer.write(b'REQUEST_NODE_TABLE\n\n')
            await writer.drain()

            # 接收数据
            node_json = await reader.readuntil(b'\n\n')
            # 将字典转换回Nde_Table对象
            self.node_table.decode(node_json)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error during request_data_table: {e}")

        writer.close()
        await writer.wait_closed()

    def print_node_table(self):
        js_node = self.node_table.encode()
        print(js_node)

    async def request_data_table(self):
        reader, writer = await asyncio.open_connection(self.server_ip, self.server_port)
        try:
            # 发送请求
            writer.write(b'REQUEST_DATA_TABLE\n\n')
            await writer.drain()

            # 接收数据
            data = await reader.readuntil(b'\n\n')
            json_data_table = json.loads(data.decode('utf-8'))
            data_dict_list = json_data_table
            # 将数据字典转换回Data对象
            self.data_table = [
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

    def print_data_table(self):
        data_dict_list = [
            {'id': data.id, 'save_hash': data.save_hash, 'title': data.title, 'path': data.path,
             'check_hash': data.check_hash, 'file_size': data.file_size}
            for data in self.data_table
        ]
        print(data_dict_list)

    async def download_from_remote(self, data: Data, timeout=1000):
        request = b'DOWNLOAD\n\n'
        try:
            reader, writer = await asyncio.open_connection(self.server_ip, self.server_port)
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
            real_data = await reader.readexactly(data.file_size)
            download_path = './download/' + data.title
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            async with aiofiles.open(download_path, 'wb') as file:
                await file.write(real_data)

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

    async def send_data(self, path: str, timeout=100):
        """
            用于将本Data对象发送至指定节点
            :param request: 发送该数据包的请求：SEND_DATA or REPLY_DOWNLOAD
            :param dest_ip: 目的ip
            :param dest_port: 目的端口，可使用缺省值8888
            :param timeout： 超时上限，缺省值100s

            """
        data = self.get_local_data(path)
        request = b'UPLOAD\n\n'
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.server_ip, self.server_port)

                # 1 Prepare non-binary data/准备非二进制数据
                send_data = {
                    "id": 0,
                    "save_hash": 0,
                    "title": data.title,
                    "path": data.path,
                    "check_hash": data.check_hash,
                    "file_size": data.file_size}
                json_data = json.dumps(send_data).encode('utf-8')
                # 2 read file
                with open(send_data['path'], 'rb') as file:
                    real_data = file.read()

                # 3 send data，收集ACK，没有收到的加入到queue中隔一段时间继续发送
                writer.write(request)
                writer.write(json_data)
                writer.write(b'\n\n')  # 使用两个换行符作为分隔符
                writer.write(real_data)
                await writer.drain()
                # Wait for ACK with a timeout
                ack = await asyncio.wait_for(reader.readuntil(b'\n\n'), timeout=timeout)

                if ack == b'SAVE_SUCCESS\n\n':
                    # print("Data sent successfully")
                    break

            except asyncio.TimeoutError:
                print("Timeout occurred when sending data. Retry in 10 seconds")
                await asyncio.sleep(10)
                print("Retrying...")
            except OSError as e:
                print(
                    f"Connection failed when sending data. Error: {e}. Retry in 10 seconds")
                await asyncio.sleep(10)
                print("Retrying...")

            finally:
                if 'writer' in locals():
                    writer.close()
                    await writer.wait_closed()

    def get_local_data(self, path):
        title = path.split('/')[-1]
        data = Data(self.cur_id+1, 0, title, path)
        return data

async def main():
    client = Client('192.168.1.101', ROOT_PORT)

    await client.request_node_table()
    client.print_node_table()

    await client.request_data_table()
    client.print_data_table()

    while True:
        command = input("Command:")
        paras = command.split(' ')
        # store path
        if paras[0] == 'store':
            path = paras[1]
            await client.send_data(path)
            print("store finished!")
        if paras[0] == 'download':
            id = int(paras[1])
            data = client.data_table[id]
            await client.download_from_remote(data)
            print("download finished!")
        if paras[0] == 'data_table':
            await client.request_data_table()
            client.print_data_table()
        if paras[0] == 'node_table':
            await client.request_node_table()
            client.print_node_table()
        if paras[0] == 'quit':
            quit = input('press [y] to quit')
            if quit == 'y':
                break
            else:
                print("continue working")


if __name__ == '__main__':
    asyncio.run(main())
