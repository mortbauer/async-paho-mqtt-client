import asyncio
from dotenv import load_dotenv
import os
import logging
from async_paho_mqtt_client import AsyncClient as amqtt

class MessageLogger:
    def __init__(self, id="dev", host="test.mosquitto.org", port=1883, username=None, password=None, tls=False, tls_insecure=True, notify_birth=False) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.client = None
        # load env vars 
        load_dotenv()
        self.MQTTS_ID = os.getenv("MQTTS_ID") if os.getenv("MQTTS_ID") else id 
        self.MQTTS_BROKER = os.getenv("MQTTS_BROKER") if os.getenv("MQTTS_BROKER") else host
        self.MQTTS_PORT = int(os.getenv("MQTTS_PORT")) if os.getenv("MQTTS_PORT") else port
        self.MQTTS_USERNAME = os.getenv("MQTTS_USERNAME") if os.getenv("MQTTS_USERNAME") else username
        self.MQTTS_PASSWORD = os.getenv("MQTTS_PASSWORD") if os.getenv("MQTTS_PASSWORD") else password
        self.tls = tls
        self.tls_insecure = tls_insecure if tls else False
        self.keepalive = 60
        self.notify_birth = notify_birth

    async def listen(self):
        client = amqtt(
            host=self.MQTTS_BROKER,
            port=self.MQTTS_PORT,
            username=self.MQTTS_USERNAME,
            password=self.MQTTS_PASSWORD,
            client_id=self.MQTTS_ID,
            tls=self.tls,
            tls_insecure=self.tls_insecure,
            keepalive=self.keepalive,
            notify_birth=self.notify_birth,
        )

        await client.start()
        await client.wait_started()
        logging.info("connected")
        self.client = client

    async def subscribe(self, topic):
        await self.client.subscribe(topic)
        self.client.message_callback_add(topic, self.on_message)

    def on_message(self, client, userdata, message):
        try:
            message_ = message.payload.decode("utf-8")
            if message_:
                logging.info(f"Received:{message_} from {message.topic} topic")
                # Do whatever you want with the message here
        except Exception as error:
            logging.error(f'on message Error "{error}"..')

    async def publish(self, topic, payload):
        self.client.publish(topic, payload)
        logging.info(f"Published: {payload} to {topic} topic")

    async def main(self):
        try:
            await self.listen()
            # example topic
            topic = "ACME_Utility/@json-scada/tags/#"
            await self.subscribe(topic)
            logging.info("subscribed")
            while True:
                # Here to simulate a blocking loop but you can avoid this 
                # if you need to make your program flow continue
                
                # logging.debug(".")
                await asyncio.sleep(1)
        except Exception as error:
            logging.error(f'main Error "{error}"..')
        finally:
            self.client.stop()
            await self.client.wait_started()


async def main():
    try:
        await asyncio.sleep(1)  # Replace with your actual main program logic
        message_logger = MessageLogger()
        await message_logger.main()

    except asyncio.exceptions.CancelledError:
        pass
    except Exception as error:
        logging.error(f'message_logger "{error}"..')


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    asyncio.run(main())
