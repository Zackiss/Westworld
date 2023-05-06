import kafka
import json

bootstrap_servers = "127.0.0.1:9092"


def produce_gen_result(user_id, serial_num, data_prev, data):
    producer = kafka.KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    msg = {
        f"{user_id}": f"{serial_num}",
        'data_content': f'{data}'
        }
    producer.send('line_gen_topic', msg)
