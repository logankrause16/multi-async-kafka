import aiokafka
import asyncio

async def consume(consumer_id):
    consumer = aiokafka.AIOKafkaConsumer(
        'my_topic',
        bootstrap_servers='127.0.0.1:9092',
        group_id='my-group'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"Consumer {consumer_id}: {msg.value.decode('utf-8')}")
    except Exception as e:
        print(f"Consumer {consumer_id} encountered an error: {e}")
    finally:
        await consumer.stop()

async def start_consumers(num_consumers):
    tasks = []
    for i in range(num_consumers):
        tasks.append(asyncio.create_task(consume(i)))
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    num_consumers = 4
    asyncio.run(start_consumers(num_consumers))
