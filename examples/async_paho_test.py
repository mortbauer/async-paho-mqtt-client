import asyncio
from async_paho_mqtt_client import AsyncClient


async def main():
    client = AsyncClient(host="test.mosquitto.org", port=1883, client_id="my-client")
    await client.start()

    await client.wait_started()

    topic = "ACME_Utility/@json-scada/tags/#"
    await client.subscribe(topic)
    client.message_callback_add(topic, on_message)

    await asyncio.sleep(10)  # Run the client for 10 seconds

    client.stop()
    await client.wait_started()

def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    print(f"Received message on topic '{message.topic}': {payload}")

if __name__ == "__main__":
    asyncio.run(main())
