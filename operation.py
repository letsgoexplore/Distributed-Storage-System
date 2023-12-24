from __future__ import annotations
import asyncio
import json

from network_layer import Node, Node_Table
from data_layer import Data, Data_Table

# 返回node_table和data_table
def setup():
    pass

# For the first one who join the network
def start_network():
    pass

async def join_network():
    pass

async def handle_join_network():
    pass

async def quit_network():
    pass

async def handle_quit_network():
    pass

async def store_data():
    pass

async def handle_store_data():
    pass

async def read_data():
    pass

async def handle_read_data():
    pass

# 返回事务类型flag，和事务信息data
def decode_message():
    pass

async def start_service():
    # step 0: initiate
    setup()

    while True:
        # step 1: wait for message
        # TODO

        # step 2: decode message
        flag, data = decode_message()

        # step 3: deal with the action
        if flag == 'join':
            asyncio.run(handle_join_network())
        elif flag == 'quit':
            asyncio.run(handle_quit_network())
        elif flag == 'store':
            asyncio.run(handle_store_data())
        elif flag == 'read':
            asyncio.run(handle_read_data())
