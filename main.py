import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request
from flask_cors import CORS
from flask_socketio import SocketIO, join_room
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


def kafka_listener(topic, group_id="admin-4", offset="latest"):
    print(f"[KAFKA] Starting listener for {topic}")

    consumer = Consumer({
        'bootstrap.servers': getenv('BOOTSTRAP_SERVER'),
        'security.protocol': getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': getenv('SASL_MECHANISMS'),
        'sasl.username': getenv('SASL_USERNAME'),
        'sasl.password': getenv('SASL_PASSWORD'),
        'group.id': group_id,
        'auto.offset.reset': offset
    })

    consumer.subscribe([topic])  

    while True:
        msg = consumer.poll(0.2)

        if msg is None or msg.error():
            continue

        try:
            data = json.loads(msg.value().decode())

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
    print(f"auth : {auth}")
    if auth:
        topic = auth.get('topic')
        driver_id = auth.get('driver_id')
        client_type = auth.get('type', 'producer')

    if not topic:
        topic = request.args.get('topic')
        driver_id = request.args.get('driver_id')

    if not topic:
        print("returning back while connecting")
        return False

    if client_type == "producer":

        with lock:
            if topic not in cached_topics:
                try:
                    fs = adminClient.create_topics([
                        NewTopic(topic, num_partitions=1, replication_factor=1)
                    ])
                    for t, f in fs.items():
                        try:
                            f.result()
                            print(f"[KAFKA] Created topic {t}")
                        except Exception as e:
                            print(f"[KAFKA] Create failed: {e}")
                except Exception as e:
                    print("Topic creation error:", e)

                cached_topics.add(topic)

        join_room(topic)
        active_topics.add(topic)

        print(f"[PRODUCER] joined {topic}")

    else:
        join_room(f"{topic}-consumer")

        with lock:
            if topic not in running_consumers:
                threading.Thread(
                    target=kafka_listener,
                    args=(topic,),
                    daemon=True
                ).start()

                running_consumers.add(topic)

        print(f"[CONSUMER] joined {topic}-consumer")


@socketio.on("loc_update-producer")
def handle_loc_update(data):
    topic = data.get('topic')
    loc = data.get('loc')
    driver = data.get('driver_id')
    school = data.get('school')

    if not topic or not loc:
        return

    try:
        producer.produce(
            topic,
            key=bytes(f"{school}-{driver}", 'utf-8'),
            value=json.dumps(loc).encode()
        )

        producer.flush(0.01)

        print(f"[PRODUCED] {topic} -> {loc}")

    except Exception as e:
        print("Producer error:", e)


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
