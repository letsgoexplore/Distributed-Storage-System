from __future__ import annotations
import shutil
import asyncio
import json

from hash_ring import HashRing
from network_layer import Node, Node_Table
from data_layer import Data, DataTable, DataServer


# 返回node_table和data_table
def setup():
    pass


# For the first one who join the network
def start_network():
    pass


async def join_network():
    pass


async def quit_network():
    pass


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


# 最后能写成抽象函数，对于任意的类型都能encode
async def encode_message():
    pass


async def start_service():
    # step 0: initiate
    setup()
    port_data_service = 8888
    data_table = DataTable()

    # 更多运行示例可见test_data_layer.py
    data_server = DataServer(data_table=data_table, node_id='node1', ring=HashRing(), ip='127.0.0.1',
                             port=port_data_service)

    # Create tasks for running servers, 这里还可以添加其他异步任务
    tasks = [
        asyncio.create_task(data_server.run())
    ]

    # Run all tasks concurrently
    await asyncio.gather(*tasks)

