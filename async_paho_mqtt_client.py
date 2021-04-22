import json
import time
import asyncio
import logging

import paho.mqtt.client as paho

class AsyncClient:
    def __init__(self,client=None,host='127.0.0.1',port=1883,loop=None,username=None,password:str=None,reconnect_interval=5,keepalive=60,ca_cert=None):
        self.logger = logging.getLogger('.'.join((__name__,host,str(port))))
        self.host = host
        self.keepalive = keepalive
        self.port = port
        self._stop = False
        self.loop = loop or asyncio.get_event_loop()
        self.reconnect_interval = reconnect_interval
        self._reconnector_loop = None
        self.client_id = username or None
        self.client = client or paho.Client(self.client_id)
        if username is not None and password is not None:
            self.client.username_pw_set(username,password)
        if ca_cert:
            self.client.tls_set(ca_cert)
        self._misc_loop = None
        self.connected = False
        self.client.will_set(f'{self.client_id}/state',json.dumps({'connected':False}))
        self.client.on_socket_open = self._on_socket_open
        self.client.on_socket_close = self._on_socket_close
        self.client.on_socket_register_write = self._on_socket_register_write
        self.client.on_socket_unregister_write = self._on_socket_unregister_write
        self.on_connect = [self.notify_birth]
        self.client.on_connect = self._handle_on_connect

    def _handle_on_connect(self, client, userdata, flags, rc, properties=None):
        for on_connect_handler in self.on_connect:
            try:
                on_connect_handler(client,userdata,flags,rc,properties=properties)
            except:
                self.logger.exception('Failed handling connect')
        self.logger.info('Connected to %s:%s',self.host,self.port)

    def _on_socket_open(self, client, userdata, sock):
        self.logger.debug("MQTT socket opened")
        def cb():
            # self.logger.debug('MQTT Socket is readable, calling loop read')
            client.loop_read()
        self.loop.add_reader(sock,cb)
        self._misc_loop = self.loop.create_task(self._create_misc_loop())
        self.connected = True

    def _on_socket_close(self, client, userdata, sock):
        self.logger.debug("MQTT socket closed")
        self.connected = False
        self.loop.remove_reader(sock)
        if self._misc_loop is not None and not self._misc_loop.done():
            self._misc_loop.cancel()
        if not self._stop:
            self.loop.create_task(self.start_reconnect_delayed(delay=self.reconnect_interval))
            self.logger.info('Scheduled reconnect in %s seconds',self.reconnect_interval)

    async def start_reconnect_delayed(self,delay=10):
        await asyncio.sleep(delay)
        await self.reconnect_loop()

    def _on_socket_register_write(self, client, userdata, sock):
        self.logger.debug("Watching MQTT socket for writability.")
        def cb():
            # self.logger.debug('MQTT Socket is writeable, calling loop write')
            client.loop_write()
        self.loop.add_writer(sock,cb)

    def _on_socket_unregister_write(self, client, userdata, sock):
        self.logger.debug("Stop watching MQTT socket for writability.")
        self.loop.remove_writer(sock)

    async def _create_misc_loop(self):
        """ misc loop need maintain state """
        self.logger.debug('Misc MQTT loop started')
        while self.client.loop_misc() == paho.MQTT_ERR_SUCCESS:
            try:
                # self.logger.debug('Misc loop sleep')
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
        self.logger.debug('Misc MQTT loop is finished')

    async def reconnect_loop(self):
        """ tries to connect forever unless stop set or connection established """
        self.logger.debug('MQTT starting reconnect loop to %s:%s',self.host,self.port)
        while not self._stop:
            try:
                self.logger.warning('MQTT connecting if no broker listens this will block for about a minute')
                self.client.connect(self.host,port=self.port,keepalive=self.keepalive)
                break
            except asyncio.CancelledError:
                break
            except:
                self.logger.warning('MQTT connect failed, sleeping %s',self.reconnect_interval)
                await asyncio.sleep(self.reconnect_interval)
        self.logger.info('MQTT finished reconnect loop')
   
    def start(self):
        if self._reconnector_loop is None or self._reconnector_loop.done():
            self._reconnector_loop = self.loop.create_task(self.reconnect_loop())

    async def wait_started(self):
        if self._reconnector_loop is not None and not self._reconnector_loop.done():
            await asyncio.wait_for(self._reconnector_loop,None)

    def stop(self):
        self.logger.info('Stopping')
        self._stop = True
        # mqtt broker will send last will since brake of unexpectedly
        sock = self.client.socket()
        if sock is not None:
            sock.close()
        if self._misc_loop is not None and not self._misc_loop.done():
            self._misc_loop.cancel()
        self.logger.info('Stopped')

    def publish(self,topic,payload,**kwargs):
        self.client.publish(f'{self.client_id}/{topic}',payload,**kwargs)

    @staticmethod
    def timestamp():
        return time.time()

    def notify_birth(self,*args,**kwargs):
        self.publish('state',json.dumps({'connected':True,'at':self.timestamp()}),retain=True)

