import json
import paho.mqtt.client as PahoMQTT

class MyMQTT:
    def __init__(self, clientID, broker, port, notifier):
        self.broker = broker
        self.port = port
        self.notifier = notifier
        self.clientID = clientID
        self._subscribed_topics = set() 
        self._isSubscriber = False

        self.client = PahoMQTT.Client(client_id=clientID, clean_session=True)
        self.client.on_connect = self.myOnConnect
        self.client.on_message = self.myOnMessage

    def myOnConnect(self, client, userdata, flags, rc):
        print(f"Connected to {self.broker} with result code: {rc}")

    def myOnMessage(self, client, userdata, msg):
        # Automatically forwards payload to the listener class notify
        self.notifier.notify(msg.topic, msg.payload.decode("utf-8"))

    def myPublish(self, topic, message):
        # Publishes data using QoS 2
        self.client.publish(topic, json.dumps(message), 2)

    def mySubscribe(self, topic):
        print(f"Subscribing to {topic}")
        result, mid = self.client.subscribe(topic, 2)
        if result == PahoMQTT.MQTT_ERR_SUCCESS:
            self._subscribed_topics.add(topic)
            self._isSubscriber = True

    def start(self):
        print("Starting MQTT client")
        self.client.connect(self.broker, self.port)
        self.client.loop_start()

    def unsubscribe(self):
        if self._subscribed_topics:
            print(f"Unsubscribing from {self._subscribed_topics}")
            self.client.unsubscribe(*self._subscribed_topics)
            self._subscribed_topics.clear()
            self._isSubscriber = False

    def stop(self):
        if self._isSubscriber:
            self.unsubscribe()
        self.client.loop_stop()
        self.client.disconnect()