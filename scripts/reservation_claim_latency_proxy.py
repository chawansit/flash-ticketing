"""Local qualification only: fixed one-way latency without serial packet sleeps."""
import asyncio
import os


async def forward(reader, writer, delay):
    queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    async def receive():
        while data := await reader.read(65536):
            queue.put_nowait((loop.time()+delay,data))
        queue.put_nowait((loop.time(),None))

    async def deliver():
        while True:
            due,data = await queue.get()
            if data is None:
                break
            await asyncio.sleep(max(0,due-loop.time()))
            writer.write(data)
            await writer.drain()
        writer.close()

    await asyncio.gather(receive(),deliver())


async def connection(reader,writer):
    remote,upstream = await asyncio.open_connection("qualified-postgres",5432)
    delay = float(os.environ["QUALIFY_WIRE_ONE_WAY_MS"])/1000
    try:
        await asyncio.gather(forward(reader,upstream,delay),forward(remote,writer,delay))
    finally:
        writer.close()
        upstream.close()


async def main():
    async with await asyncio.start_server(connection,"0.0.0.0",5432) as server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
