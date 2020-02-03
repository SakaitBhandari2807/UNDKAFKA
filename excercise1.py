import asyncio

from confluent_kafka import Consumer, Producer

from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "first-python-kafka-client"

config = {"bootstrap.server": BROKER_URL}


async def producer(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL, "group.id": "my-first-ccode"})
    curr_iteration = 0
    while True:
        p.produce(topic_name, f"MESSAGE: {curr_iteration}".encode("UTF-8"))
        curr_iteration += 1
        await asyncio.sleep(1)


async def consumer(topic_name):
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "my-first-ccode"})
    c.subscribe([topic_name])

    while True:
        message = c.poll(1.0)
        if message is None:
            print("No message found")
        elif message.error():
            print("ERROR")
        else:
            print(f"MESSAGE {message.key()}: {message.value()}")
        await asyncio.sleep(1)


async def produce_consume():
    t1 = asyncio.create_task(producer(TOPIC_NAME))
    t2 = asyncio.create_task(consumer(TOPIC_NAME))
    await t1
    await t2


def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
    client.create_topics([topic])
    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("Shutting Down")
    finally:
        client.delete_topics([TOPIC_NAME])


if __name__ == "__main__":
    main()

