import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, join_room
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from os import getenv
import json
import threading

load_dotenv()

app = Flask(__name__)
CORS(app)

# Socket.IO setup with eventlet
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

# Global State
active_topics = set()
running_consumers = set()
cached_topics = set()
lock = threading.Lock()
active_listeners = {} 


def create_topic_safe(topic_name):
    """
    Runs outside the main Eventlet loop. Safely checks and creates topics.
    """
    try:
        # Check if topic already exists to avoid unnecessary error throwing
        metadata = adminClient.list_topics(timeout=5)
        if topic_name in metadata.topics:
            print(f"[KAFKA] Topic {topic_name} already exists in broker.")
            return

        print(f"[KAFKA] Attempting to create topic: {topic_name}")
        fs = adminClient.create_topics([
            NewTopic(topic_name, num_partitions=1, replication_factor=1)
        ])
        
        for t, f in fs.items():
            try:
                f.result()  # This blocks, but it's safe inside tpool
                print(f"[KAFKA] Successfully created topic {t}")
            except Exception as e:
                # Handle race conditions where topic was created moments ago
                if hasattr(e, 'args') and len(e.args) > 0 and hasattr(e.args[0], 'code'):
                    if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                        print(f"[KAFKA] Topic {t} already exists.")
                    else:
                        print(f"[KAFKA] Create failed: {e}")
                else:
                    print(f"[KAFKA] Create failed: {e}")
    except Exception as e:
        print(f"[KAFKA] Admin client error: {e}")


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

    while active_listeners.get(topic, True):
        eventlet.sleep(0.01) 
        
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

    print(f"[KAFKA] Stopping listener and closing consumer for {topic}")
    consumer.close()
    
    with lock:
        running_consumers.discard(topic)
        active_listeners.pop(topic, None)


@socketio.on('connect')
def handle_connect(auth):
    print("Client connected : ", auth)

    topic = None
    driver_id = None
    client_type = "producer"

    if auth:
        topic = auth.get('topic')
        driver_id = auth.get('driver_id')
        client_type = auth.get('type', 'producer')

    if not topic:
        topic = request.args.get('topic')
        print("Topic from query params:", topic)

    if not topic:
        print("No topic provided, disconnecting client")
        return False  # Reject connection cleanly
        
    if client_type == "producer":
        needs_creation = False
        
        with lock:
            if topic not in cached_topics:
                cached_topics.add(topic)
                needs_creation = True

        if needs_creation:
            # Offload blocking C-extension call to a native thread
            eventlet.tpool.execute(create_topic_safe, topic)

        join_room(topic)
        active_topics.add(topic)
        print(f"[PRODUCER] joined {topic}")

    else:
        join_room(f"{topic}-consumer")

        with lock:
            if topic not in running_consumers:
                active_listeners[topic] = True 
                
                socketio.start_background_task(
                    kafka_listener,
                    topic
                )
                running_consumers.add(topic)
            else:
                print(f"[KAFKA] Listener for {topic} already running")

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
        # poll(0) handles callbacks asynchronously without blocking the event loop
        producer.poll(0) 
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
            active_listeners[topic] = True 
            
            socketio.start_background_task(
                kafka_listener,
                topic,
                group_id,
                offset
            )
            running_consumers.add(topic)
        if topic not in active_topics:
            active_topics.add(topic)
        else:
            return f"Someone is already consuming {topic}. Please try again after sometime. Till then check other rides", 400

    print(f"[ADMIN] Started consuming {topic}")
    return render_template('ride.html')


@app.route('/stop_listening', methods=['POST'])
def stop_listening():
    topic = request.args.get('topic')
    
    if not topic:
        return jsonify({"error": "No topic provided"}), 400

    with lock:
        if topic in active_listeners:
            active_listeners[topic] = False 
            return jsonify({"status": f"Sent stop signal to {topic}"}), 200
        else:
            return jsonify({"status": f"No active listener found for {topic}"}), 404


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)