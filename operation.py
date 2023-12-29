from __future__ import annotations
import shutil
import asyncio
import json
import os
import aiofiles
import socket

from hash_ring import HashRing
from network_layer import Node, Node_Table
from data_layer import Data, DataTable, StorageServer


# 返回node_table和data_table
def setup():
    # 从json文件加载节点表
    Node_Table(initial_nodes=None, new_start=False)


def start_network():
    # 获取本机ip地址
    # ip = "192.168.52.1"
    ip = socket.gethostbyname(socket.gethostname())
    initial_node = Node(parent_ip=ip, ip=ip)

    # 构建初始化节点表，节点表被保存在json文件
    node_table = Node_Table(initial_nodes=initial_node)


async def join_network(node_table, dest_port=8888):
    # 获取本机ip地址
    # ip = "192.168.52.1"
    ip = socket.gethostbyname(socket.gethostname())
    # 手动选取父亲节点ip，也可以改为探查所有存在的节点后选取
    parent_ip = "192.168.52.1"

    data = ip + "/" + parent_ip
    request = b'JOIN\n\n'
    # 向目标服务器发送请求
    try:
        for node in node_table:
            if node.ip != ip:
                reader, writer = await asyncio.open_connection(node.ip, dest_port)
                writer.write(request)
                writer.write(data.encode('utf-8'))
                writer.write(b'\n\n')  # Using two newline characters as a separator

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


async def quit_network(node_table, dest_port):
    # 获取本机ip地址
    # ip = "192.168.52.1"
    ip = socket.gethostbyname(socket.gethostname())
    # 手动选取父亲节点ip，也可以改为探查所有存在的节点后选取

    data = ip
    request = b'QUIT\n\n'
    # 向目标服务器发送请求
    try:
        for node in node_table:
            if node.ip != ip:
                reader, writer = await asyncio.open_connection(node.ip, dest_port)
                writer.write(request)
                writer.write(data.encode('utf-8'))
                writer.write(b'\n\n')  # Using two newline characters as a separator

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


async def read_data():
    pass


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


# 最后能写成抽象函数，对于任意的类型都能encode
async def encode_message():
    pass


async def start_service():
    # step 0: initiate
    setup()
    port_data_service = 8888
    data_table = DataTable()

    # 更多运行示例可见test_data_layer.py
    storage_server = StorageServer(data_table=data_table, node_id='node1', ring=HashRing(), ip='127.0.0.1',
                                port=port_data_service)

    # Create tasks for running servers, 这里还可以添加其他异步任务
    tasks = [
        asyncio.create_task(storage_server.run())
    ]

    # Run all tasks concurrently
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(start_service())
