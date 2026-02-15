import asyncio
import sys
import uuid
from pathlib import Path

import aio_pika
import aio_pika.abc

from async_consumer.config import get_connection
from async_consumer.tracer import Tracer

sys.path.insert(0, str(Path(__file__).parent))


class Publisher:

    def __init__(self, routing_key: str, connection: aio_pika.abc.AbstractConnection):
        self.connection = connection
        self.routing_key = routing_key
        self.tracer = Tracer("publisher-service")

    async def produce_message(self, message: str) -> None:
        span = self.tracer.start_span("publish_message")
        span.add_attribute("routing_key", self.routing_key)
        span.add_attribute("message_length", len(message))
        async with self.connection.channel() as channel:
            rmq_message = aio_pika.Message(
                body=message.encode(),
                message_id=f"msg-{uuid.uuid4()}",
                headers={
                    "x-source": "order-service",
                    "x-timestamp": asyncio.get_event_loop().time(),
                },
            )

            rmq_message = self.tracer.add_trace_context_to_message(rmq_message, span)

            await channel.default_exchange.publish(rmq_message, routing_key=self.routing_key)

            print(f"Message sent: {message[:50]}...")
            print(f"  trace_id: {span.trace_id}")
            print(f"  span_id: {span.span_id}")
            print(f"  traceparent: {self.tracer.create_traceparent(span.trace_id, span.span_id)}")

        self.tracer.end_span(span)

    async def close(self) -> None:
        for span in self.tracer.spans:
            print(f"  {span.name}: {span.duration_ms():.2f}ms")
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
