# import eventlet
# eventlet.monkey_patch()

from flask import Flask, render_template, request
from flask_cors import CORS
from flask_socketio import SocketIO, join_room
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient

from os import getenv
import json
import asyncio
import threading

load_dotenv()

app = Flask(__name__)
CORS(app)

socketio = SocketIO(app, cors_allowed_origins="*")

connectedUsers = {}

config = {
    'bootstrap.servers': getenv('bootstrap.servers'),
    'security.protocol': getenv('security.protocol'),
    'sasl.mechanisms': getenv('sasl.mechanisms'),
    'sasl.username': getenv('sasl.username'),
    'sasl.password': getenv('sasl.password'),
    'client.id': getenv('client.id'),
}

producer = Producer(config)

adminClient = AdminClient(config)

active_topics = set()

async def kafka_listener(topic, offset="latest",group_id="admin-4"):
    consumer = Consumer({
        'bootstrap.servers': getenv('bootstrap.servers'),
        'security.protocol': getenv('security.protocol'),
        'sasl.mechanisms': getenv('sasl.mechanisms'),
        'sasl.username': getenv('sasl.username'),
        'sasl.password': getenv('sasl.password'),
        'group.id': group_id,
        'auto.offset.reset': offset
    })
    
    while True:
        consumer.subscribe([topic])
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        try:
            data = json.loads(msg.value().decode())
            topic = msg.topic()

            print(f"[KAFKA] {topic} -> {data}")

            socketio.emit(
                "location_update",
                data,
                room=f"{topic}-consumer"
            )

        except Exception as e:
            print("Parse error:", e)


@socketio.on('connect')
def handle_connect(auth):
    print("Client connected")

    topic = None
    driver_id = None
    client_type = "producer"

    if auth:
        topic = auth.get('topic')
        driver_id = auth.get('driver_id')
        client_type = auth.get('type', 'producer')

    if not topic:
        topic = request.args.get('topic')
        driver_id = request.args.get('driver_id')

    if not topic:
        return False

    if client_type == "producer":
        if topic not in connectedUsers:
            connectedUsers[topic] = set()

        connectedUsers[topic].add(driver_id)

        join_room(topic)
        
        active_topics.add(topic)

        print(f"[PRODUCER] joined {topic}")

    else:
        join_room(f"{topic}-consumer")
        print(f"[CONSUMER] joined {topic}-consumer")


@socketio.on("loc_update-producer")
def handle_loc_update(data):
    topic = data.get('topic')
    loc = data.get('loc')

    if not topic or not loc:
        return

    try:
        producer.produce(
            topic,
            key=b"key",
            value=json.dumps(loc).encode()
        )

        producer.poll(0)

        print(f"[PRODUCED] {topic} -> {loc}")

    except Exception as e:
        print("Producer error:", e)


@app.route('/admin')
def admin():
    topic = request.args.get('topic')
    group_id = request.args.get('group_id', 'admin-4')
    offset = request.args.get('offset', 'latest')
    
    if topic is None and group_id == 'admin-4' and offset == 'latest':
        print("No parameters provided, rendering index.html")
        metadata = adminClient.list_topics(timeout=10)
        
        return render_template('index.html', topics=metadata.topics.keys())
    
    if topic is None:
        return "Topic is required", 400
    
    threading.Thread(target=lambda: asyncio.run(kafka_listener(topic=topic,group_id=group_id,offset=offset)), daemon=True).start()
    print(f"Started Kafka listener for topic: {topic} with group_id: {group_id} and offset: {offset}")
    return render_template('ride.html')



if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)