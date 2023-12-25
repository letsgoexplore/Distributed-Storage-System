# import package
from __future__ import annotations
import asyncio
import json


class Data:
    # path: '../storage/xxxxx'
    def __init__(self, id, save_hash, title, path, check_hash=0):
        # here, 'file' refer to the local address of the file
        #       'locations' refers to which place the file is saved
        # for key, value in locals().items():
        #     if key != "self":
        #         setattr(self, key, value)
        # 废除了上面的初始化方式，使用了如下显式的初始化
        self.id = id
        self.save_hash = save_hash
        self.title = title
        self.path = path
        self.check_hash = check_hash

    # !!!!!
    def __eq__(self, other:Data):
        if isinstance(other, Data):
            return self.path == other.path
        return False

    # TODO
    async def send_data(self, ip, port,timeout=100):
        while True:
            try:
                reader, writer = await asyncio.open_connection(ip, port)

                ## 1 Prepare non-binary data/准备非二进制数据
                data = {
                    "id": self.id,
                    "save_hash": self.save_hash,
                    "title": self.title,
                    "path": self.path,
                    "check_hash": self.check_hash}
                json_data = json.dumps(data).encode('utf-8')
                ## 2 read file
                # currently, it's reading from local address
                with open(self.path, 'rb') as file:
                    pdf_data = file.read()

                ## 3 send data，收集ACK，没有收到的加入到queue中隔一段时间继续发送
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
                print("Timeout occurred. Retry in 10 seconds")
                await asyncio.sleep(10)
                print("Retrying...")
            except OSError as e:
                print(f"Connection failed. Error: {e}. Retry in 10 seconds")
                await asyncio.sleep(10)
                print("Retrying...")

            finally:
                if 'writer' in locals():
                    writer.close()
                    await writer.wait_closed()

    # TODO
    # function: using Consistent Hashing Algorithm to decide whether to save
    def need_to_save(self, node_id):
        pass

    # currently assume no transmission error
    def verify_file_with_check_hash(self):
        return True


## AT: pr
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
            self.datas = json_data_table

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error during request_data_table: {e}")

        writer.close()
        await writer.wait_closed()
