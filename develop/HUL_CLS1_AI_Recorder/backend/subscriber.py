# subscriber.py
import zmq
import zmq.asyncio
import asyncio
import json
from datetime import datetime

ZMQ_ADDR = "tcp://localhost:8011"

async def subscriber():
    print("[Subscriber] initializing")
    ctx = zmq.asyncio.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(ZMQ_ADDR)
    sock.setsockopt_string(zmq.SUBSCRIBE, "")
    print(f"[Subscriber] connected to {ZMQ_ADDR}, subscribed to all topics")

    while True:
        try:
            msg = await sock.recv_json()
            recv_ts = datetime.utcnow().isoformat()
            size_bytes = len(json.dumps(msg).encode("utf-8"))       # measure size (bytes)
            keys = list(msg.keys())
            print(f"[Subscriber] {recv_ts} - Received message. Top-level keys: {keys}. Size: {size_bytes} bytes")
    
            for k, v in msg.items():
                try:
                    
                    s = len(json.dumps(v).encode("utf-8")) # if v(value) is dict with machines 
                    print(f"  - key={k}, payload_size={s} bytes")
                except Exception:
                    pass

            dump = json.dumps(msg)
            print("[Subscriber] payload (first 800 chars):")
            print(dump[:800])
            print("---- end partial payload ----")
        except Exception as e:
            print("[Subscriber] error:", e)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(subscriber())
