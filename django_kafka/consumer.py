import logging
import time

from confluent_kafka import Consumer, Message
from django.conf import settings
from django.db import connection

from django_kafka.errors import (
    ConnectionClosedException,
    DatabaseShutDownException,
    RestartConsumerError,
)
from django_kafka.producer import producer

# Configura o logger específico para a sua biblioteca
logger = logging.getLogger(__name__)

KAFKA_RUNNING: bool = True


def ensure_connection():
    if connection.connection is not None and connection.connection.closed:
        connection.close()
    try:
        connection.ensure_connection()
    except Exception as e:
        logger.error(f"Error connecting to DB: {str(e)}")
        raise RestartConsumerError("Failed to connect to DB.")


def __kafka_consumer_run() -> None:

    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVER,
        "group.id": settings.KAFKA_GROUP_ID,
        "auto.offset.reset": settings.KAFKA_OFFSET_RESET
        if hasattr(settings, "KAFKA_OFFSET_RESET")
        else "earliest",
    }

    consumer: Consumer = Consumer(conf)
    topics: list[str] = [key for key, _ in settings.KAFKA_TOPICS.items()]

    consumer.subscribe(topics)

    try:
        while KAFKA_RUNNING:
            msg: Message = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                # print("Consumer error: {}".format(msg.error()))
                logger.error("Consumer error: {}".format(msg.error()))
                continue

            callback: str = settings.KAFKA_TOPICS.get(msg.topic())

            if callback is None:
                # print("No callback found for topic: {}".format(msg.topic()))
                logger.error("No callback found for topic: {}".format(msg.topic()))
                continue

            # call the callback string as function
            dynamic_call_action(callback, consumer, msg)
    except RestartConsumerError:
        try:
            consumer.close()
        except Exception as e:
            # print(e)
            logger.error(e)
        raise RestartConsumerError("Restart consumer please")
    except Exception as e:
        raise Exception(f"Unexpected error: {e}")
    finally:
        consumer.close()
        raise RestartConsumerError("Restart consumer please")


def kafka_consumer_run():
    attempts = 0
    while True:
        try:
            if attempts < 10:
                attempts += 1
            else:
                print("Too many attempts, stopping consumer.")
                break
            print("Verify db connection")
            ensure_connection()
            print("Initing consumer")
            __kafka_consumer_run()
            attempts = 0
            logger.info("Consumer stopped.")
            break
        except RestartConsumerError:
            print("Restarting consumer...")
            time.sleep(10 * attempts)
            continue
        except Exception as e:
            print("unexpected exception", e)
            continue


def kafka_consumer_shutdown() -> None:
    global KAFKA_RUNNING
    KAFKA_RUNNING = False


def dynamic_call_action(action: str, consumer: Consumer, msg: Message) -> None:

    # get path removing last part splited by dot
    module_path: str = ".".join(action.split(".")[:-1])

    # get path keeping last part splited by dot
    function_name: str = action.split(".")[-1]

    import importlib
    try:
        module = __import__(module_path, fromlist=[function_name])
        # reload o módulo para garantir que alterações sejam refletidas
        module = importlib.reload(module)
    except Exception as e:
        logger.error(f"No module found or error reloading for action: {action} - {e}")
        return

    # get function from module
    try:
        function = getattr(module, function_name)
    except Exception as e:
        logger.error(f"No function found for action: {action} - {e}")
        return

    # call function
    try:
        function(consumer=consumer, msg=msg)
    except DatabaseShutDownException as e:
        logger.error("Error connecting to database:", str(e))
        print("Produzindo novamente a mensagem:", msg.topic(), msg.value())
        producer(msg.topic(), msg.value())
        raise RestartConsumerError("Restart consumer please")
    except ConnectionClosedException as e:
        logger.error("Error connecting to Kafka:", str(e))
        producer(msg.topic(), msg.value())
        raise RestartConsumerError("Restart consumer please")
    except Exception as e:
        # print("Error calling action: {}".format(action))
        logger.error("Error calling action: {}".format(action))
        raise e
