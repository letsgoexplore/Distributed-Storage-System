from __future__ import annotations
import asyncio
from hash_ring import HashRing
from data_layer import Data, DataTable, DataServer


# 程序的初始化及调用示例如下

async def main():
    port_data_service = 8888
    data_table = DataTable()
    data_server = DataServer(data_table=data_table, node_id='node1', ring=HashRing(), ip='127.0.0.1', port=port_data_service)

    # Create tasks for running servers
    tasks = [
        asyncio.create_task(data_server.run())
    ]

    # Run all tasks concurrently
    await asyncio.gather(*tasks)


# 测试使用的模拟任务，用于验证数据收发功能、数据表请求及发送功能，尚未验证
async def test_task(data_table: DataTable):
    data = Data(id=0, save_hash=0, title='test.pdf', path='./files/test.pdf')
    # data_table.add_data(data)
    await data.send_data('127.0.0.1', 8888)
    print('send success!')
    data2 = Data(id=0, save_hash=0, title='send_test.py', path='./files/send_test.py')
    # data_table.add_data(data2)
    await data2.send_data('127.0.0.1', 8888)
    await data_table.request_data_table('127.0.0.1', 8888)
    print(data_table.datas)


if __name__ == "__main__":
    asyncio.run(main())



