import time
import threading
from concurrent.futures import ThreadPoolExecutor
import rest_client
import mqtt_client
from data_buffer import DataBuffer

WORKER_THREAD_COUNT = 20  # Fixed number of worker threads

buffer = DataBuffer(
    batch_size=10,
    max_wait_seconds=5
)

worker_pool = ThreadPoolExecutor(
    max_workers=WORKER_THREAD_COUNT,
    thread_name_prefix="iot-worker"
)

def process_message(message):
    try:
        buffer.add(message)
    except Exception as e:
        print(f"Error processing message: {e}")

def mqtt_message_callback(message):
    worker_pool.submit(process_message, message)

def batch_sender_loop():
    while True:
        try:
            batch = buffer.get_batch_if_ready()
            if batch:
                worker_pool.submit(rest_client.send_to_cloud, batch)
        except Exception as e:
            print(f"Error sending batch: {e}")
        
        time.sleep(1)  # check every second

def main():   
    # MQTT client listener
    mqtt_thread = threading.Thread(
        target=mqtt_client.start_mqtt,
        args=(mqtt_message_callback,),
    )
    mqtt_thread.start()
    print(f"Started MQTT listener with {WORKER_THREAD_COUNT} worker threads")

    rest_thread = threading.Thread(
        target=batch_sender_loop,
    )
    rest_thread.start()

    mqtt_thread.join()
    rest_thread.join()
        
if __name__ == "__main__":
    main()
