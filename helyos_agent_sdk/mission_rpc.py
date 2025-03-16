import uuid
import json
import time

class MissionRPCConnector:
    """
    This class handles RPC calls for requesting missions from helyOS Core.
    It connects to the HelyOS client via AMQP and sends a mission request,
    waiting synchronously for a response containing the mission id.

    Attributes:
        connection (obj): The underlying AMQP connection from the helyOS client.
        helyos_client (obj): The helyOS client object.
        routing_key (str): The routing key for mission requests.
        channel (obj): The AMQP channel.
        callback_queue (str): The exclusive queue for receiving RPC responses.
        response (str): The response received from the RPC call.
        corr_id (str): The unique correlation id for the RPC call.

    Example:
        helyos_client = HelyOSClient("dev2.rabbitmq.net", uuid="01234-01234-01234")
        helyos_client.connect(username="01234-01234-01234", password="secret_password")
        helyos_client.perform_checkin(yard_uid="0", agent_data={'name': "my truck", 'factsheet': {}})
        helyos_client.get_checkin_result()
        
        mission_rpc = MissionRPCConnector(helyos_client)
        mission_id = mission_rpc.call(
            mission_name="ExtraAssignment",
            data={"task": "deliver package"},
            agent_uuids=["agent1", "agent2"],
            signed=True
        )
        print("Received mission ID from helyOS core:", mission_id)
    """
    
    def __init__(self, helyos_client):
        if helyos_client._protocol == 'MQTT':
            raise Exception('Remote procedure call should use AMQP protocol.')
        self.connection = helyos_client.connection
        self.routing_key = helyos_client.mission_routing_key
        self.helyos_client = helyos_client

        # Create a channel and declare an exclusive callback queue.
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # Set up the consumer on the callback queue.
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, mission_name, data, agent_uuids=[], signed=False, timeout=10):
        # Ensure the connection is open.
        if not self.helyos_client.is_connection_open:
            self.helyos_client.reconnect()
            time.sleep(3)
            self.__init__(self.helyos_client)

        self.response = None
        self.corr_id = str(uuid.uuid4())

        payload = {
            'type': 'MISSION',
            'uuid': self.helyos_client.uuid,
            'body': {
                'work_process_type_name': mission_name,
                'data': data,
                'agent_uuids': agent_uuids,
                'yard_uid': self.helyos_client.yard_uid,
            }
        }
        message = json.dumps(payload, sort_keys=True)

        self.helyos_client.publish(
            routing_key=self.routing_key,
            message=message,
            signed=signed,
            reply_to=self.callback_queue,
            corr_id=self.corr_id
        )

        # Wait for the response with a timeout.
        start_time = time.time()
        while self.response is None:
            self.connection.process_data_events(time_limit=5)
            if time.time() - start_time > timeout:
                raise TimeoutError("Mission request RPC timed out.")
        
        response_data = json.loads(json.loads(self.response)['message'])
        return response_data
