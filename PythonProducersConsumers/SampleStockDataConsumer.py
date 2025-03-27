from kafka import KafkaConsumer
import logging
import signal
import sys

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("PythonConsumer")


def create_consumer():
    consumer = KafkaConsumer(
        'topics.test.v1',
        bootstrap_servers='localhost:9092',
        group_id='python-test-stock-consumer',
        key_deserializer=bytes.decode,
        value_deserializer=bytes.decode,
        auto_offset_reset='earliest'
    )
    return consumer

def graceful_shutdown(signal, frame, consumer):
    log.info("Shutting down")
    consumer.close()
    sys.exit(0)

if __name__ == "__main__":
    consumer = create_consumer()

    signal.signal(signal.SIGINT, lambda sig, frame: graceful_shutdown(sig, frame, consumer))
    signal.signal(signal.SIGTERM, lambda sig, frame: graceful_shutdown(sig, frame, consumer))

    log.info("Consumer started")
    try:
        while True:
            log.info("Polling for data.")
            for message in consumer:
                log.info(f"Received message: key={message.key},"
                             f" value={message.value},"
                             f" topic={message.topic},"
                             f" partition={message.partition},"
                             f" offset={message.offset}")

    except Exception as e:
        log.error("Error while consuming: {e}")
    finally:
        consumer.close()
        log.info("Consumer closed")