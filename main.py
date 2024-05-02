import aiokafka

async def main():
    consumer = aiokafka.AIOKafkaConsumer(
        'my_topic',
        bootstrap_servers='localhost:9092',
        group_id='my-group')
    await consumer.start()
    try:
        async for msg in consumer:
            print(msg)
    finally:
        await consumer.stop()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())