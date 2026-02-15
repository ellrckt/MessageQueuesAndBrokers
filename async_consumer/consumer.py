import asyncio
import random
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aio_pika
import aio_pika.abc as pabc

from async_consumer.config import (
    RMQ_DL_EXCHANGE,
    RMQ_DL_QUEUE,
    RMQ_RETRY_EXCHANGE,
    RMQ_RETRY_QUEUE,
    get_connection,
)
from async_consumer.tracer import Span, Tracer

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))


class GracefullConsumer:
    """
    The consumer operates with at-least-once delivery semantics:
    Graceful shutdown
    Signal handlers enable controlled termination without message loss
    Dead Letter Queue (DLQ) for failed messages
    Retry mechanism with exponential backoff
    Cross-platform support (Windows & Linux)
    """

    def __init__(self, routing_key: str, connection: pabc.AbstractConnection) -> None:
        self.routing_key = routing_key
        self.connection = connection
        self.channel: pabc.AbstractChannel
        self.consumer_tag: str
        self.queue: pabc.AbstractQueue
        self.shutdown_event = asyncio.Event()
        self.in_flight_messages = 0
        self.max_retries = 3
        self.base_delay = 1
        self.max_delay = 30
        self.dl_queue: pabc.AbstractQueue
        self.dl_exchange: pabc.AbstractExchange
        self.retry_queue: pabc.AbstractQueue
        self.retry_exchange: pabc.AbstractExchange
        self.tracer = Tracer("consumer-service")

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

        self.retry_exchange = await self.channel.declare_exchange(
            RMQ_RETRY_EXCHANGE, type=pabc.ExchangeType.DIRECT, durable=True
        )

        self.retry_queue = await self.channel.declare_queue(
            RMQ_RETRY_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self.routing_key,
            },
        )

        await self.retry_queue.bind(self.retry_exchange, routing_key=f"{self.routing_key}.retry")

        self.dl_exchange = await self.channel.declare_exchange(
            RMQ_DL_EXCHANGE, type=pabc.ExchangeType.DIRECT, durable=True
        )

        self.dl_queue = await self.channel.declare_queue(RMQ_DL_QUEUE, durable=True)

        await self.dl_queue.bind(self.dl_exchange, RMQ_DL_QUEUE)

        self.queue = await self.channel.declare_queue(
            self.routing_key,
            durable=True,
            arguments={
                "x-dead-letter-exchange": RMQ_RETRY_EXCHANGE,
                "x-dead-letter-routing-key": f"{self.routing_key}.retry",
            },
        )

        self.consumer_tag = await self.queue.consume(self.consume_message)
        if sys.platform != "win32":
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(
                    sig, lambda s=sig: asyncio.create_task(self.signal_handler())
                )
        else:
            asyncio.create_task(self.windows_shutdown_monitor())

    async def windows_shutdown_monitor(self):
        try:
            while not self.shutdown_event.is_set():
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    async def signal_handler(self):
        self.shutdown_event.set()

    def calculate_backoff(self, retry_count: int) -> int:
        delay = self.base_delay * (2**retry_count)
        return min(delay, self.max_delay)

    async def send_to_dlq(
        self, message: pabc.AbstractIncomingMessage, reason: str, retry_count: int, trace_span: Span
    ):
        headers = dict(message.headers) if message.headers else {}
        headers.update(
            {
                "x-error-reason": reason,
                "x-error-time": datetime.now(timezone.utc),
                "x-original-routing-key": self.routing_key,
                "x-final-retry-count": retry_count,
            }
        )
        dlq_span = self.tracer.start_span(
            "send_to_dlq",
            incoming_traceparent=self.tracer.create_traceparent(
                trace_span.trace_id, trace_span.span_id
            ),
        )
        dlq_span.add_attribute("reason", reason)
        dlq_span.add_attribute("retry_count", retry_count)

        dlq_message = aio_pika.Message(
            body=message.body,
            headers=headers,
            delivery_mode=pabc.DeliveryMode.PERSISTENT,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            correlation_id=message.correlation_id,
            reply_to=message.reply_to,
            message_id=message.message_id,
            timestamp=message.timestamp,
        )
        dlq_message = self.tracer.add_trace_context_to_message(dlq_message, dlq_span)

        await self.dl_exchange.publish(
            dlq_message,
            routing_key=RMQ_DL_QUEUE,
        )
        self.tracer.end_span(dlq_span)
        print(f"Message sent to DLQ. Reason: {reason}, Retries: {retry_count}")

    async def retry_message(
        self, message: pabc.AbstractIncomingMessage, retry_count: int, trace_span: Span
    ):
        delay_seconds = self.calculate_backoff(retry_count)
        delay_ms = delay_seconds

        headers = dict(message.headers) if message.headers else {}
        headers.update(
            {
                "x-retry-count": retry_count + 1,
                "x-retry-delay": delay_seconds,
                "x-last-retry-time": datetime.utcnow().isoformat(),
                "x-original-routing-key": self.routing_key,
            }
        )
        retry_span = self.tracer.start_span(
            "schedule_retry",
            incoming_traceparent=self.tracer.create_traceparent(
                trace_span.trace_id, trace_span.span_id
            ),
        )
        retry_span.add_attribute("retry_number", retry_count + 1)
        retry_span.add_attribute("delay_seconds", delay_seconds)

        retry_message = aio_pika.Message(
            body=message.body,
            headers=headers,
            delivery_mode=pabc.DeliveryMode.PERSISTENT,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            correlation_id=message.correlation_id,
            reply_to=message.reply_to,
            message_id=message.message_id or f"msg-{datetime.utcnow().isoformat()}",
            timestamp=message.timestamp,
            expiration=int(delay_ms),
        )

        retry_message = self.tracer.add_trace_context_to_message(retry_message, retry_span)

        await self.retry_exchange.publish(
            retry_message,
            routing_key=f"{self.routing_key}.retry",
        )
        self.tracer.end_span(retry_span)
        print(f"Message scheduled for retry #{retry_count + 1} with {delay_seconds}s delay")

    def _safe_int_conversion(self, value: Any, default: int = 0) -> int:
        if value is None:
            return default

        if isinstance(value, (int, float)):
            return int(value)

        if isinstance(value, (str, bytes, bytearray)):
            try:
                return int(value)
            except (ValueError, TypeError):
                return default

        return default

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
        traceparent = self.tracer.get_trace_context_from_message(message)
        main_span = self.tracer.start_span("consume_message", traceparent)
        self.in_flight_messages += 1
        try:
            headers = message.headers or {}
            retry_count_value = headers.get("x-retry-count", 0)
            retry_count = self._safe_int_conversion(retry_count_value)
            main_span.add_attribute("attempt", retry_count + 1)
            main_span.add_attribute("routing_key", self.routing_key)
            main_span.add_attribute("message_id", message.message_id)
            print(f"Processing message (attempt #{retry_count + 1}/{self.max_retries + 1})")
            process_span = self.tracer.start_span(
                "process_message",
                incoming_traceparent=self.tracer.create_traceparent(
                    main_span.trace_id, main_span.span_id
                ),
            )
            if random.random() > 0.3:
                await asyncio.sleep(5)
                process_span.add_attribute("result", "success")
                self.tracer.end_span(process_span)
                print("Successfully processed")
                await message.ack()
            else:
                raise Exception("Processing failed")

        except Exception as e:
            if "process_span" in locals():
                process_span.add_attribute("error", str(e))
                self.tracer.end_span(process_span)

            main_span.add_attribute("error", str(e))
            headers = message.headers or {}
            retry_count = self._safe_int_conversion(headers.get("x-retry-count", 0))

            if retry_count < self.max_retries:
                await self.retry_message(message, retry_count, main_span)
                await message.ack()
                print(f"Retry scheduled ({retry_count + 1}/{self.max_retries})")
            else:
                await self.send_to_dlq(
                    message, f"Max retries ({self.max_retries}) exceeded", retry_count, main_span
                )
                await message.ack()
                print("Max retries exceeded, sent to DLQ")

        finally:
            self.tracer.end_span(main_span)
            self.in_flight_messages -= 1

    async def shutdown(self):
        print(f"Shutting down. Waiting for {self.in_flight_messages} in-flight messages...")

        if self.consumer_tag and self.queue:
            await self.queue.cancel(self.consumer_tag)

        timeout = 30
        while self.in_flight_messages > 0 and timeout > 0:
            await asyncio.sleep(1)
            timeout -= 1

        if self.in_flight_messages > 0:
            print(f"Warning: {self.in_flight_messages} messages still in flight")

        for span in self.tracer.spans:
            print(f"  {span.name}: {span.duration_ms():.2f}ms (trace: {span.trace_id[:8]}...)")

        if self.channel:
            await self.channel.close()

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
