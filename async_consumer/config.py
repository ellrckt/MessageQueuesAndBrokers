import aio_pika
import aio_pika.abc
from decouple import config

RMQ_HOST = config("RMQ_HOST")
RMQ_PORT = config("RMQ_PORT", cast=int)

RABBITMQ_DEFAULT_USER = config("RABBITMQ_DEFAULT_USER")
RABBITMQ_DEFAULT_PASS = config("RABBITMQ_DEFAULT_PASS")

RMQ_DL_EXCHANGE = "dlx-exchange"
RMQ_DL_QUEUE = "dlq_internship"

RMQ_RETRY_EXCHANGE = "retry-exchange"
RMQ_RETRY_QUEUE = "retry_internship"


async def get_connection() -> aio_pika.abc.AbstractConnection:
    connection = await aio_pika.connect_robust(
        host=RMQ_HOST,
        port=RMQ_PORT,
        login=RABBITMQ_DEFAULT_USER,
        password=RABBITMQ_DEFAULT_PASS,
        virtualhost="/",
    )
    return connection
