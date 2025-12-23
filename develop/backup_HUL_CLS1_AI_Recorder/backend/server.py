import asyncio
import nats
import json
import os
from dataclasses import asdict, dataclass



@dataclass
class Response:
    plc: str
    command:str
    ack:bool

@dataclass
class Payload:
    plc:str
    name:str
    value:float
    status:bool
    command:str

async def run():
    # Connect to the NATS server
    nc = await nats.connect("nats://192.168.0.170:4222")

    # Define the request handler
    async def message_handler(msg):
        print("message",msg)
        request = Payload(**json.loads(msg.data))
        print(request)
        payload = Response(plc="22", command="TOGGLE",ack=True)
        bytes_ = json.dumps(asdict(payload)).encode()
        await msg.respond(bytes_)

    # Subscribe to a subject and handle requests
    await nc.subscribe("adv.150", cb=message_handler)

    print("Server is listening for requests on 'greeting'...")
    
    # Keep the server running
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(run())
