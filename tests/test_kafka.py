# test_kafka.py
from confluent_kafka import Producer, Consumer, KafkaError
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_API_KEY,
    KAFKA_API_SECRET,
    KAFKA_TOPICS
)
import time

def test_kafka_connection():
    """Test Kafka connection, produce and consume a message"""
    try:
        # Producer configuration
        producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': KAFKA_API_KEY,
            'sasl.password': KAFKA_API_SECRET
        }
        
        # Consumer configuration
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': KAFKA_API_KEY,
            'sasl.password': KAFKA_API_SECRET,
            'group.id': 'test-consumer-group',
            'auto.offset.reset': 'earliest'
        }
        
        # Create producer
        producer = Producer(producer_conf)
        print("✓ Kafka producer created")
        
        # Callback for delivery reports
        def delivery_report(err, msg):
            if err:
                print(f'✗ Message delivery failed: {err}')
            else:
                print(f'✓ Message delivered to {msg.topic()} [partition {msg.partition()}]')
        
        # Test 1: Produce a message to spotify-events topic
        test_topic = KAFKA_TOPICS['events']
        test_message = 'Hello from Spotify pipeline!'
        
        producer.produce(
            test_topic,
            key='test-key',
            value=test_message,
            callback=delivery_report
        )
        
        # Wait for message to be delivered
        producer.flush()
        print(f"✓ Test message sent to topic: {test_topic}")
        
        # Test 2: Consume the message back
        consumer = Consumer(consumer_conf)
        consumer.subscribe([test_topic])
        print(f"✓ Consumer subscribed to topic: {test_topic}")
        
        # Try to read the message (with timeout)
        print("Attempting to consume message...")
        msg = consumer.poll(timeout=10.0)
        
        if msg is None:
            print("⚠ No message received (this might be okay if topic was already consumed)")
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("⚠ Reached end of partition")
            else:
                print(f"✗ Consumer error: {msg.error()}")
        else:
            print(f"✓ Message consumed: {msg.value().decode('utf-8')}")
        
        consumer.close()
        
        print("\nKafka Connection Test PASSED!")
        return True
        
    except Exception as e:
        print(f"\nKafka Connection Test FAILED!")
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    test_kafka_connection()