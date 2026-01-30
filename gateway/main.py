import time
import threading
import rest_client
import mqtt_client
from data_buffer import DataBuffer

buffer = DataBuffer(
    batch_size=10,
    max_wait_seconds=5
)

def mqtt_message(message):
    buffer.add(message)

def main():
    # Create thread for mqtt_client
    mqtt_thread = threading.Thread(target=mqtt_client.start_mqtt, args=(mqtt_message,), daemon=True)
    mqtt_thread.start()

    # try to get batches from databuffer to add to database via rest_client
    while True:
        batch = buffer.get_batch_if_ready()

        if batch:
            success = rest_client.send_to_cloud(batch)
            if not success:
                print("Re-queueing failed batch")
                buffer.requeue(batch)

        time.sleep(1)

if __name__ == "__main__":
    main()
