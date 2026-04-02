from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from flask_cors import CORS
from os import urandom, getenv
from flask_socketio import SocketIO, send, join_room, leave_room
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer
import json

app = Flask(__name__)
CORS(app)

load_dotenv()

socketio = SocketIO(app, cors_allowed_origins="*")
connectedUsers = set()

room = "kafka-default"

config = {
    'bootstrap.servers': getenv('bootstrap.servers'),
    'security.protocol': getenv('security.protocol'),
    'sasl.mechanisms': getenv('sasl.mechanisms'),
    'sasl.username': getenv('sasl.username'),
    'sasl.password': getenv('sasl.password'),
    'client.id': getenv('client.id'),
    'session.timeout.ms': getenv('session.timeout.ms')
}

producer = Producer(config)

@socketio.on('connect')
def handle_connect(data):
    global room
    print('Client connected')
    connectedUsers.add(data['driver_id'])
    received_topic = data.get('topic')
    if received_topic:
        room = received_topic
        print(f"Joining room: {room}")
    join_room(room)
    print(f"Connected users: {len(connectedUsers)}")
    

@socketio.on("loc_update")
def handle_loc_update(data):
    print(f"Received location update: {data}")
    topic = data.get('topic', room)
    loc = data.get('loc')
    if loc:
        key = "key"
        value = loc
        producer.produce(topic, key=key.encode(),value=json.dumps(loc).encode())
        print(f"Produced message to topic {topic}: key = {key} value = {value}")

        producer.flush()
        
        
if __name__ == '__main__':
    socketio.run(app, host='localhost', port=5000)
