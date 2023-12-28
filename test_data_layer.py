from __future__ import annotations
import asyncio
import shutil
from hash_ring import HashRing
from data_layer import Data, DataTable, DataServer
import os
import json
import aiofiles


async def download_from_remote(data: Data, ip, port, timeout=1000):
    request = b'DOWNLOAD\n\n'
    try:
        reader, writer = await asyncio.open_connection(ip, port)

        data0 = {
            "id": data.id,
            "save_hash": data.save_hash,
            "title": data.title,
            "path": data.path,
            "check_hash": data.check_hash,
            "file_size": data.file_size}
        json_data = json.dumps(data0).encode('utf-8')

        ## 3 send data，收集ACK，没有收到的加入到queue中隔一段时间继续发送
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


# 程序的初始化及调用示例如下
async def main():
    port_data_service = 8888
    data_table = DataTable()
    os.makedirs(os.path.dirname('./storage/'), exist_ok=True)
    os.makedirs(os.path.dirname('./download/'), exist_ok=True)
    ring = HashRing(['1', '2', '3'])
    data_server = DataServer(data_table=data_table, node_id='node1', ring=ring, ip='127.0.0.1', port=port_data_service)

    # Create tasks for running servers
    tasks = [
        asyncio.create_task(data_server.run())
    ]

    # Run all tasks concurrently
    await asyncio.gather(*tasks)


# 测试使用的模拟任务，在另一个实例上跑的任务，用于验证数据收发功能、数据表请求及发送功能，验证完成
async def test_task(data_table:DataTable):
    server_ip = '127.0.0.1'
    server_port = 8888
    os.makedirs(os.path.dirname('./storage/'), exist_ok=True)
    os.makedirs(os.path.dirname('./download/'), exist_ok=True)
    data = Data(id=0, save_hash=0, title='test.pdf', path='./files/test.pdf')
    shutil.copy('./files/test.pdf', data.save_path)
    # data_table.add_data(data)
    await data.send_data(server_ip, server_port)
    print('send success!')
    data2 = Data(id=1, save_hash=0, title='send_test.py', path='./files/send_test.py')
    shutil.copy('./files/send_test.py', data2.save_path)
    # data_table.add_data(data2)
    await data2.send_data(server_ip, server_port)
    await data_table.request_data_table(server_ip, server_port)
    await download_from_remote(data2, server_ip, server_port)
    print(data_table.datas)


if __name__ == "__main__":
    asyncio.run(main())



