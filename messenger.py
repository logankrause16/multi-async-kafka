import aiokafka
import asyncio

async def produce():
    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers='127.0.0.1:9092'
    )
    await producer.start()
    try:
        for i in range(10):
            message = f"message {i}"
            await producer.send_and_wait("my_topic", message.encode('utf-8'))
            print(f"Sent: {message}")
    except Exception as e:
        print(f"Producer encountered an error: {e}")
    finally:
        await producer.stop()

if __name__ == '__main__':
    asyncio.run(produce())
