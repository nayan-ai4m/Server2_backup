import nats
import asyncio

async def test_nats():
    try:
        nc = await nats.connect(
            "nats://192.168.1.149:4222",
            connect_timeout=5,
            max_reconnect_attempts=3,
            reconnect_time_wait=1
        )
        print("Connected successfully!")
        await nc.close()
    except Exception as e:
        print(f"Error: {e}")

asyncio.run(test_nats())
