import asyncio
import json
from data_layer import Data, DataTable


class DataReceiveServer:
    def __init__(self, port=8888):
        self.port = port
        self.received_data_queue = asyncio.Queue()

    async def handle_receive(self, reader, writer):
        # Read JSON data
        data = await reader.readuntil(b'\n\n')
        json_data = json.loads(data.decode('utf-8'))
        pdf_data = await reader.read()
        received_data = Data(
            id=json_data['id'],
            save_hash=json_data['save_hash'],
            title=json_data['title'],
            path=json_data['path'],
            check_hash=json_data['check_hash'])
        await self.received_data_queue.put(received_data)
        with open(json_data['path'], 'wb') as file:
            file.write(pdf_data)

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


class DataTableSendServer:
    def __init__(self, port=8889, data_table: DataTable = DataTable()):
        self.port = port
        self.data_table = data_table

    async def handle_client(self, reader, writer):
        try:
            request = await reader.readuntil(b'\n')
            if request == b'REQUEST_DATA_TABLE\n':
                print("Client requested Data_Table")
                json_data_table = json.dumps(self.data_table.datas).encode('utf-8')
                writer.write(json_data_table)
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