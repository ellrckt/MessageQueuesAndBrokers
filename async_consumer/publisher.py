import asyncio
import sys
from pathlib import Path

import aio_pika
import aio_pika.abc

from async_consumer.config import get_connection

sys.path.insert(0, str(Path(__file__).parent))


class Publisher:

    def __init__(self, routing_key: str, connection: aio_pika.abc.AbstractConnection):
        self.connection = connection
        self.routing_key = routing_key

    async def produce_message(self, message: str) -> None:
        async with self.connection.channel() as channel:
            await channel.default_exchange.publish(
                aio_pika.Message(body=message.encode()), routing_key=self.routing_key
            )

    async def close(self) -> None:
        if not self.connection.is_closed:
            await self.connection.close()


async def main() -> None:
    routing_key = "internship"
    message = "Hello"
    connection = await get_connection()
    try:
        publisher = Publisher(routing_key=routing_key, connection=connection)

        for i in range(3):
            await publisher.produce_message(message)
            print(f"Message {i+1} sent")
            await asyncio.sleep(0.5)

    finally:
        await publisher.close()


if __name__ == "__main__":
    asyncio.run(main())
