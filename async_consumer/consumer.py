import asyncio
import signal
from typing import Optional

import aio_pika.abc as pabc

from .config import get_connection


class GracefullConsumer:
    """
    The consumer operates with at-least-once delivery semantics:
    Messages are acknowledged only after successful processing
    In case of failure, the message is requeued
    Signal handlers enable controlled termination without message loss
    """

    def __init__(self, routing_key: str, connection: pabc.AbstractConnection) -> None:
        self.routing_key = routing_key
        self.connection = connection
        self.channel: Optional[pabc.AbstractChannel] = None
        self.consumer_tag: Optional[str] = None
        self.queue: Optional[pabc.AbstractQueue] = None
        self.shutdown_event = asyncio.Event()
        self.in_flight_messages = 0

    async def setup(self):
        """
        Initialize the consumer and prepare for message processing.

        This method sets up the complete consumer infrastructure:
        - Opens a dedicated channel for communication with RabbitMQ
        - Configures QoS with prefetch_count=1 to limit unacknowledged messages
        - Declares a durable queue to survive broker restarts
        - Registers the message handler and obtains a consumer tag for management
        - Installs signal handlers for graceful shutdown
        """

        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)
        self.queue = await self.channel.declare_queue(self.routing_key, durable=True)

        self.consumer_tag = await self.queue.consume(self.consume_message)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.signal_handler()))

    async def signal_handler(self):
        self.shutdown_event.set()

    async def consume_message(self, message: pabc.AbstractIncomingMessage) -> None:
        """
        Also you can use special context processor.
        (  async with message.process() )
        If context processor will catch an exception,
        the message will be returned to the queue.
        Using this code you can be sure that even if you kill
        a worker using CTRL+C while it was processing a message,
        nothing will be lost.
        Soon after the worker dies all unacknowledged messages
        will be redelivered.
        """
        try:

            self.in_flight_messages += 1
            print("Processing message")
            await asyncio.sleep(5)
            print("Successfully processed")
            await message.ack()
        except Exception:
            await message.nack(requeue=True)
            print("An error occured")
        finally:
            self.in_flight_messages -= 1

    async def shutdown(self):

        if self.consumer_tag and self.queue:
            await self.queue.cancel(self.consumer_tag)

        while self.in_flight_messages > 0:
            await asyncio.sleep(1)

    async def run(self):
        await self.setup()
        try:
            await self.shutdown_event.wait()
        except asyncio.CancelledError:
            print("Bye")
        finally:
            await self.shutdown()


async def main():
    routing_key = "internship"
    connection = await get_connection()
    consumer = GracefullConsumer(routing_key=routing_key, connection=connection)
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
