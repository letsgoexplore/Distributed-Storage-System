from __future__ import annotations
import asyncio
from data_layer import Data, DataReceiveServer, DataTableService, process_received_data


# 程序的初始化及调用示例如下

async def main():
    port_data_table = 8889
    port_data_receive = 8888

    data_table = DataTableService(port=port_data_table)
    data_receive_server = DataReceiveServer(port=port_data_receive)

    # Create tasks for running servers
    tasks = [
        asyncio.create_task(data_table.run()),
        asyncio.create_task(data_receive_server.run()),
        asyncio.create_task(process_received_data(node=0, data_receive_server=data_receive_server, data_table=data_table))
    ]

    # Run all tasks concurrently
    await asyncio.gather(*tasks)

# 测试使用的模拟任务，用于验证数据收发功能、数据表请求及发送功能，已验证完成
async def test_task(data_table:DataTableService):
    data = Data(id=0, save_hash=0, title='test.pdf', path='./files/test.pdf')
    # data_table.add_data(data)
    await data.send_data('127.0.0.1', 8888)
    print('send success!')
    data2 = Data(id=0, save_hash=0, title='send_test.py', path='./files/send_test.py')
    # data_table.add_data(data2)
    await data2.send_data('127.0.0.1', 8888)
    await data_table.request_data_table('127.0.0.1', 8889)
    print(data_table.datas)


if __name__ == "__main__":
    asyncio.run(main())



