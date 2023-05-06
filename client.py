import json
import sys
import threading
import kafka
import requests

threads = []
cookie = "token=code_space;"
header = {
    "cookie": cookie,
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/92.0.4515.159 Safari/537.36"
}


class PullThread(threading.Thread):
    def __init__(self, thread_name, topic, partition):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.partition = partition
        self.topic = topic

    def run(self):
        print("Starting " + self.name)
        set_consumer(self.thread_name, self.topic, self.partition)

    @staticmethod
    def stop():
        sys.exit()


def set_consumer(thread_name, topic, partition):
    broker_list = brokers
    # set topic and partition, construct object tp
    tp = kafka.TopicPartition(topic, partition)
    consumer = kafka.KafkaConsumer(
        bootstrap_servers=broker_list,
        group_id="robot",
        client_id=thread_name,
        enable_auto_commit=False,
        fetch_min_bytes=1024 * 1024,  # 1M
        fetch_max_wait_ms=60000,  # 30s
        request_timeout_ms=305000,
    )
    # assign consumer to partition
    consumer.assign([tp])
    # get offset of last consuming
    offset = consumer.end_offsets([tp])[tp]
    # set current consuming offset
    consumer.seek(tp, offset)
    print(u"Running at thread:", thread_name, u"Partition:", partition,
          u"With offset:", offset, u"\tConsuming...")

    consume_num = 0
    while True:
        msg = consumer.poll(timeout_ms=60000)  # 30s
        end_offset = consumer.end_offsets([tp])[tp]
        if len(msg) > 0:
            print(u"At thread:", thread_name, u"Partition:", partition,
                  u"Max offset:", end_offset, u"With Data:", len(msg))
            line_num = 0
            for data in msg.values():
                for line in data:
                    response = {"status": 200}
                    print("Received:", line)
                    user_id = 0
                    serial_num = 0
                    line_num += 1
                    '''
                    We shall generate next line here with given "line"
                    '''
                    gen_line = line
                    print("Generated:", line)
                    post_dict = {
                        f"{user_id}": f"{serial_num}",
                        'data_content': f'{gen_line}'
                    }
                    while response["status"] != "200":
                        response = requests.post(receiver, data=post_dict, headers=header, cookies=cookie)
            print(thread_name, ", total lines:", line_num)
            # submit the offset to broker server
            consumer.commit(offsets={tp: (kafka.OffsetAndMetadata(end_offset, None))})
            if not 0:
                # shut down this sub-thread
                sys.exit()
        else:
            print(thread_name, 'no data fetched')
        consume_num += 1
        print(thread_name, "Consuming at times:", consume_num)


if __name__ == '__main__':
    thread_num = 3
    brokers = "127.0.0.1:9092"
    receiver = "127.0.0.1:5000"
    # "ip:9092,127.0.0.1:9092"
    for i in range(thread_num):
        threads.append(PullThread(f"Thread-{i}", "line_gen_topic", i))
    for t in threads:
        t.start()
    # let main thread wait for all sub-threads ending
    for t in threads:
        t.join()
