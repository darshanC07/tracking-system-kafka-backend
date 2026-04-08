import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request
from flask_cors import CORS
from flask_socketio import SocketIO, join_room,emit
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from os import getenv
import json
import threading


load_dotenv()

app = Flask(__name__)
CORS(app)

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")


config = {
    'bootstrap.servers': getenv('BOOTSTRAP_SERVER'),
    'security.protocol': getenv('SECURITY_PROTOCOL'),
    'sasl.mechanisms': getenv('SASL_MECHANISMS'),
    'sasl.username': getenv('SASL_USERNAME'),
    'sasl.password': getenv('SASL_PASSWORD'),
    'client.id': getenv('CLIENT_ID'),
}

producer = Producer(config)
adminClient = AdminClient(config)


active_topics = set()
running_consumers = set()
cached_topics = set()
lock = threading.Lock()

def create_consumer(topic):
    consumer = Consumer({
        "bootstrap.servers": getenv('BOOTSTRAP_SERVER'),
        "group.id": topic,
        "auto.offset.reset": "latest"
    })
    consumer.subscribe([topic])
    return consumer

# def kafka_listener(topic, group_id="admin-4", offset="latest"):
#     print(f"[KAFKA] Starting listener for {topic}")

#     consumer = Consumer({
#         'bootstrap.servers': getenv('BOOTSTRAP_SERVER'),
#         'security.protocol': getenv('SECURITY_PROTOCOL'),
#         'sasl.mechanisms': getenv('SASL_MECHANISMS'),
#         'sasl.username': getenv('SASL_USERNAME'),
#         'sasl.password': getenv('SASL_PASSWORD'),
#         'group.id': group_id,
#         'auto.offset.reset': offset
#     })

#     consumer.subscribe([topic])  

#     while True:
#         msg = consumer.poll(0.2)

#         if msg is None or msg.error():
#             continue

#         try:
#             data = json.loads(msg.value().decode())

#             print(f"[KAFKA] {topic} -> {data}")

#             socketio.emit(
#                 "location_update",
#                 data,
#                 room=f"{topic}-consumer"
#             )

#         except Exception as e:
#             print("Parse error:", e)

def kafka_listener(topic, sid):
    consumer = create_consumer(topic)
    while True:
        msg = consumer.poll(0.2)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode())
        socketio.emit("location_update", data, to=sid)

@socketio.on("connect")
def handle_connect(auth):
    topic = auth.get("topic")
    driver = auth.get("driver_id")
    role = auth.get("type")

    if role == "producer":
        emit("connected", {"status": "ok"})
    else:
        socketio.start_background_task(kafka_listener, topic, request.sid)

@socketio.on("send_location")
def handle_location(data):
    topic = data.get("topic")
    loc = data.get("loc")
    producer.produce(topic, value=json.dumps(loc).encode())
    producer.poll(0)

@socketio.on("disconnect")
def handle_disconnect():
    print("disconnecting")
    pass

@app.route('/')
def home():
    return "Server running"

@app.route('/admin')
def admin():
    topic = request.args.get('topic')
    group_id = request.args.get('group_id', 'admin-4')
    offset = request.args.get('offset', 'latest')

    if not topic:
        metadata = adminClient.list_topics(timeout=5)
        return render_template('index.html', topics=metadata.topics.keys())

    with lock:
        if topic not in running_consumers:
            threading.Thread(
                target=kafka_listener,
                args=(topic, group_id, offset),
                daemon=True
            ).start()

            running_consumers.add(topic)

    print(f"[ADMIN] Started consuming {topic}")

    return render_template('ride.html')


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)
