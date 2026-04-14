import asyncio
import json
import threading
from os import getenv
import os  
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Kafka config
config = {
    'bootstrap.servers': getenv('BOOTSTRAP_SERVER'),
    'security.protocol': getenv('SECURITY_PROTOCOL'),
    'sasl.mechanisms': getenv('SASL_MECHANISMS'),
    'sasl.username': getenv('SASL_USERNAME'),
    'sasl.password': getenv('SASL_PASSWORD'),
    'client.id': getenv('CLIENT_ID')
}

producer = Producer(config)
adminClient = AdminClient(config)

# Global state
running_consumers = set()
active_listeners = {}
cached_topics = set()
lock = threading.Lock()

class ConnectionManager:
    def __init__(self):
        self.rooms = {}

    async def connect(self, websocket: WebSocket, room: str):
        await websocket.accept()
        self.rooms.setdefault(room, []).append(websocket)

    def disconnect(self, websocket: WebSocket):
        for room in list(self.rooms.keys()):
            if websocket in self.rooms[room]:
                self.rooms[room].remove(websocket)

    async def send_to_room(self, room: str, message: dict):
        for ws in self.rooms.get(room, []):
            await ws.send_json(message)

manager = ConnectionManager()

def create_topic_blocking(topic_name):
    try:
        metadata = adminClient.list_topics(timeout=5)

        if topic_name in metadata.topics:
            print(f"[KAFKA] Topic exists: {topic_name}")
            return

        print(f"[KAFKA] Creating topic: {topic_name}")

        futures = adminClient.create_topics([
            NewTopic(topic_name, num_partitions=1, replication_factor=3)
        ])

        for topic, future in futures.items():
            try:
                future.result(timeout=10)
                print(f"[KAFKA] Created topic: {topic}")
            except Exception as e:
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    print(f"[KAFKA] Already exists: {topic}")
                else:
                    raise e

    except Exception as e:
        print(f"[KAFKA ERROR]: {e}")
        raise e


async def kafka_listener(topic, group_id, offset):
    print(f"[KAFKA] Listening: {topic}")

    consumer = Consumer({
        **config,
        'group.id': group_id,
        'auto.offset.reset': offset
    })

    consumer.subscribe([topic])

    while active_listeners.get(topic, True):
        await asyncio.sleep(0.05)

        msg = consumer.poll(0.2)
        if msg is None or msg.error():
            continue

        try:
            data = json.loads(msg.value().decode())
            print(f"[KAFKA] {topic} -> {data}")
            await manager.send_to_room(f"{topic}-consumer", data)

        except Exception as e:
            print("Parse error:", e)

    consumer.close()

    with lock:
        running_consumers.discard(topic)
        active_listeners.pop(topic, None)


async def kafka_producer(data):
    topic = data.get("topic")
    loc = data.get("loc")
    driver = data.get("driver_id")
    school = data.get("school")

    if not topic or not loc:
        return

    create_topic_blocking(topic)

    producer.produce(
        topic,
        key=f"{school}-{driver}".encode(),
        value=json.dumps(loc).encode()
    )

    producer.flush()  

    print(f"[PRODUCED] {topic} -> {loc}")


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    topic: str = Query(None),
    group_id: str = Query(None),
    offset: str = Query(None),
    client_type: str = Query("producer"),
):
    if not topic:
        await websocket.close()
        return

    room = topic if client_type == "producer" else f"{topic}-consumer"

    await manager.connect(websocket, room)
    print(f"[{client_type.upper()}] connected to {room}")

    # Consumer start
    if client_type == "consumer":
        with lock:
            if topic not in running_consumers:
                active_listeners[topic] = True
                asyncio.create_task(kafka_listener(topic,group_id,offset))
                running_consumers.add(topic)

    try:
        while True:
            data = await websocket.receive_json()
            print("Received:", data)

            if client_type == "producer":
                await kafka_producer(data)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Client disconnected")


@app.get("/")
def home():
    return {"status": "Server running"}


@app.get("/admin")
async def admin(request: Request, topic: str = None):
    if topic is None:
        metadata = adminClient.list_topics(timeout=5)

        return templates.TemplateResponse(
            name="index.html",
            request=request,
            context=
            {
                "topics": list(metadata.topics.keys())
            }
        )

    # with lock:
    #     if topic not in running_consumers:
    #         active_listeners[topic] = True
    #         asyncio.create_task(kafka_listener(topic))
    #         running_consumers.add(topic)

    return templates.TemplateResponse(
        name="ride.html",
        request=request,
        context=
        {
            "topic": topic
        }
    )


@app.post("/stop_listening")
def stop_listening(topic: str):
    with lock:
        if topic in active_listeners:
            active_listeners[topic] = False
            return {"status": f"Stopping {topic}"}

    return JSONResponse(
        content={"error": "No active listener"},
        status_code=404
    )
    
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
