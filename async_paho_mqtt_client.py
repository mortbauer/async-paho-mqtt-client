import json
import time
from time import strftime, localtime
import asyncio
import logging
import ssl
import paho.mqtt.client as paho


class AsyncClient:
    def __init__(
        self,
        client=None,
        host="127.0.0.1",
        port=1883,
        client_id=None,
        username=None,
        password: str = None,
        reconnect_interval=5,
        keepalive=60,
        tls=False,
        tls_insecure=False,
        ca_certs=None,
        certfile=None,
        keyfile=None,
        cert_reqs=ssl.CERT_NONE,
        tls_version=ssl.PROTOCOL_TLSv1_2,
        ciphers=None,
        state_key="state",
        loop=None,
        notify_birth=False
    ):
        self.logger = logging.getLogger(".".join((__name__, host, str(port))))
        self.host = host
        self.keepalive = keepalive
        self.port = port
        self._stop = False
        self.loop = loop or asyncio.get_event_loop()
        self.reconnect_interval = reconnect_interval
        self._reconnector_loop = None
        self.client_id = client_id or None
        self.client = client or paho.Client(self.client_id)
        
        if tls:
            self.client.tls_set(ca_certs, certfile, keyfile, 
                                cert_reqs, tls_version, ciphers)
            if tls_insecure:
                self.client.tls_insecure_set(True)

        if username is not None and password is not None:
            self.client.username_pw_set(username, password)
        self._misc_loop = None
        self.connected = False
        if notify_birth:
            self.on_connect = [self.notify_birth]
        else:
            self.on_connect = []
        self.state_key = state_key
        self.on_disconnect = []
        self.client.will_set(
            f"{self.client_id}/{state_key}",
            json.dumps({"connected": False}),
            retain=True,
        )
        self.client.on_socket_open = self._on_socket_open
        self.client.on_socket_close = self._on_socket_close
        self.client.on_socket_register_write = self._on_socket_register_write
        self.client.on_socket_unregister_write = self._on_socket_unregister_write
        self.client.on_connect = self._handle_on_connect
        self.client.on_disconnect = self._handle_on_disconnect

    def _handle_on_connect(self, *args, **kwargs):
        for on_connect_handler in self.on_connect:
            try:
                res = on_connect_handler(*args, **kwargs)
                if asyncio.iscoroutine(res):
                    self.loop.create_task(res)
            except Exception as error:
                self.logger.exception(f"Failed handling connect {error}")
        self.logger.info("Connected to %s:%s", self.host, self.port)

    async def subscribe(self, *args, **kwargs):
        self.client.subscribe(*args, **kwargs)

    def message_callback_add(self, *args, **kwargs):
        self.client.message_callback_add(*args, **kwargs)

    def _handle_on_disconnect(self, *args, **kwargs):
        for on_disconnect_handler in self.on_disconnect:
            try:
                res = on_disconnect_handler(*args, **kwargs)
                if asyncio.iscoroutine(res):
                    self.loop.create_task(res)
            except Exception as error:
                self.logger.exception(f"Failed handling disconnect {error}")
        self.logger.warning("Disconnected from %s:%s", self.host, self.port)

    def _on_socket_open(self, client, userdata, sock):
        self.logger.debug("MQTT socket opened")

        def cb():
            # self.logger.debug('MQTT Socket is readable, calling loop read')
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self._misc_loop = self.loop.create_task(self._create_misc_loop())
        self.connected = True

    def _on_socket_close(self, client, userdata, sock):
        self.logger.debug("MQTT socket closed")
        self.connected = False
        self.loop.remove_reader(sock)
        if self._misc_loop is not None and not self._misc_loop.done():
            self._misc_loop.cancel()
        if not self._stop:
            self.loop.create_task(
                self.start_reconnect_delayed(delay=self.reconnect_interval)
            )
            self.logger.info(
                "Scheduled reconnect in %s seconds", self.reconnect_interval
            )

    async def start_reconnect_delayed(self, delay=10):
        await asyncio.sleep(delay)
        await self.reconnect_loop()

    def _on_socket_register_write(self, client, userdata, sock):
        self.logger.debug("Watching MQTT socket for writability.")

        def cb():
            # self.logger.debug('MQTT Socket is writeable, calling loop write')
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def _on_socket_unregister_write(self, client, userdata, sock):
        self.logger.debug("Stop watching MQTT socket for writability.")
        self.loop.remove_writer(sock)

    async def _create_misc_loop(self):
        """misc loop need maintain state"""
        self.logger.debug("Misc MQTT loop started")
        while self.client.loop_misc() == paho.MQTT_ERR_SUCCESS:
            try:
                # self.logger.debug('Misc loop sleep')
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
        self.logger.debug("Misc MQTT loop is finished")

    async def reconnect_loop(self):
        """tries to connect forever unless stop set or connection established"""
        self.logger.debug("MQTT starting reconnect loop to %s:%s", self.host, self.port)
        while not self._stop:
            try:
                self.logger.warning(
                    "MQTT connecting if no broker listens this will block for about a minute"
                )
                self.client.connect(self.host, port=self.port, keepalive=self.keepalive)
                break
            except asyncio.CancelledError:
                break
            except Exception as error:
                self.logger.warning(
                    "MQTT connect failed, sleeping %s", self.reconnect_interval
                )
                await asyncio.sleep(self.reconnect_interval)
        self.logger.info("MQTT finished reconnect loop")

    async def start(self):
        self.loop = asyncio.get_running_loop()
        if self._reconnector_loop is None or self._reconnector_loop.done():
            self._reconnector_loop = self.loop.create_task(self.reconnect_loop())

    async def wait_started(self):
        if self._reconnector_loop is not None and not self._reconnector_loop.done():
            await asyncio.wait_for(self._reconnector_loop, None)

    def stop(self):
        self.logger.info("Stopping")
        self._stop = True
        # mqtt broker will send last will since brake of unexpectedly
        sock = self.client.socket()
        if sock is not None:
            sock.close()
        if self._misc_loop is not None and not self._misc_loop.done():
            self._misc_loop.cancel()
        self.logger.info("Stopped")

    async def publish(self, topic, payload, **kwargs):
        self.client.publish(f"{self.client_id}/{topic}", payload, **kwargs)

    @staticmethod
    def timestamp():
        time_format = strftime("%Y-%m-%d %H:%M:%S", localtime(time.time()))
        return time_format

    async def notify_birth(self, *args, **kwargs):
        await self.publish(
            self.state_key,
            json.dumps({"connected": True, "at": self.timestamp()}),
            retain=True,
        )
