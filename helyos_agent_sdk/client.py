import time, warnings
from functools import wraps
import pika
import os
import json
import ssl
from .exceptions import *
from helyos_agent_sdk.models import AGENT_STATE, CheckinResponseMessage
from .crypto import Signing, generate_private_public_keys

AGENTS_UL_EXCHANGE = os.environ.get(
    'AGENTS_UL_EXCHANGE', 'xchange_helyos.agents.ul')
AGENTS_DL_EXCHANGE = os.environ.get(
    'AGENTS_DL_EXCHANGE', 'xchange_helyos.agents.dl')
AGENT_ANONYMOUS_EXCHANGE = os.environ.get(
    'AGENT_ANONYMOUS_EXCHANGE', 'xchange_helyos.agents.anonymous')
REGISTRATION_TOKEN = os.environ.get(
    'REGISTRATION_TOKEN', '0000-0000-0000-0000-0000')


def connect_rabbitmq(rabbitmq_host, rabbitmq_port, username, passwd, enable_ssl=False, ca_certificate=None, vhost='/', temporary=False):
    credentials = pika.PlainCredentials(username, passwd)
    if enable_ssl:
        if rabbitmq_port == 5672:
            warnings.warn('Warning: SSL is enabled, but the port is set to 5672, which is the default for non-encrypted AMQP connection.' +
                          ' Consider using port 5671.', UserWarning)

        context = ssl.create_default_context(cadata=ca_certificate)
        if ca_certificate is not None:
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        ssl_options = pika.SSLOptions(context, rabbitmq_host)
    else:
        ssl_options = None

    if temporary:
        params = pika.ConnectionParameters(rabbitmq_host,  rabbitmq_port, vhost,
                                          credentials,
                                           heartbeat=60,
                                           blocked_connection_timeout=60,
                                           ssl_options=ssl_options)
    else:
        params = pika.ConnectionParameters(rabbitmq_host,  rabbitmq_port, vhost,
                                           credentials,
                                           heartbeat=3600,
                                           ssl_options=ssl_options)
    _connection = pika.BlockingConnection(params)
    return _connection



class HelyOSClient():

    def __init__(self, rabbitmq_host, rabbitmq_port=5672, uuid=None, enable_ssl=False, ca_certificate=None,
                 helyos_public_key=None, agent_privkey=None, agent_pubkey=None, vhost='/' ):
        """ HelyOS client class

            The client implements several functions to facilitate the
            interaction with RabbitMQ. It reads the RabbitMQ exchange names from environment variables
            and it provides the helyOS routing-key names as properties. For SSL connections, the RabbitMQ server 
            CA certificate should be provided as a string in PEM format. If the agent public and private keys are not provided,
            they are generated by the client at the initialization, and the public key is sent to helyOS during the check-in procedure.
            If the helyOS public key is not provided, it is retrieved during the check-in procedure.

            :param rabbitmq_host: RabbitMQ host name (e.g rabbitmq.mydomain.com)
            :type rabbitmq_host: str
            :param rabbitmq_port: RabbitMQ port, defaults to 5672
            :type rabbitmq_port: int
            :param uuid: universal unique identifier fot the agent
            :type uuid: str
            :param enable_ssl: Enable rabbitmq SSL connection, default False.
            :type enable_ssl: bool, optional
            :param ca_certificate: Certificate authority of the RabbitMQ server, defaults to None
            :type ca_certificate: string (PEM format), optional
            :param helyos_public_key: helyOS RSA public key to verify the helyOS message signature.
            :type helyos_public_key:  string (PEM format), optional
            :param agent_privkey: Agent RSA private key, defaults to None
            :type agent_privkey:  string (PEM format), optional
            :param agent_pubkey: Agent RSA public key is saved in helyOS core, defaults to None
            :type agent_pubkey:  string (PEM format), optional

        """
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.ca_certificate = ca_certificate
        self.helyos_public_key = helyos_public_key
        self.uuid = uuid
        self.enable_ssl = enable_ssl
        self.vhost = vhost

        self.connection = None
        self.channel = None
        self.checkin_data = None
        self.checkin_guard_interceptor = lambda *args, **kwargs: True
        self._protocol = 'AMQP'

        self.tries = 0
        self.is_reconecting = False
        self.rbmq_username = None
        self.rbmq_password = None

        if agent_pubkey is None or agent_privkey is None:
            self.private_key, self.public_key = generate_private_public_keys()
        else:
            self.private_key, self.public_key = agent_privkey, agent_pubkey

        self.signing_helper = Signing(self.private_key)

        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port

    @property
    def is_connection_open(self):
        """ Check if the connection is open """
        try:
            self.connection.sleep(0.01)
        except:
            return False 
        return self.connection.is_open

    @property
    def checking_routing_key(self):
        """ Routing key value used for check in messages """
        return f'agent.{self.uuid}.checkin'

    @property
    def status_routing_key(self):
        """ Routing key value used to publish agent and assigment states  """

        return f'agent.{self.uuid}.state'

    @property
    def sensors_routing_key(self):
        """ Routing key value used for broadingcasting of positions and sensors  """

        return f'agent.{self.uuid}.visualization'

    @property
    def mission_routing_key(self):
        """ Routing key value used to publish mission requests  """

        return f'agent.{self.uuid}.mission_req'

    @property
    def summary_routing_key(self):
        """ Routing key value used to publish summary requests  """

        return f'agent.{self.uuid}.summary_req'

    @property
    def database_routing_key(self):
        """ Routing key value used to publish summary requests  """

        return f'agent.{self.uuid}.database_req'
    
    @property
    def yard_visualization_routing_key(self):
        """ Routing key value used to broadcast yard visualization data """
        if self.yard_uid:
            return f'yard.{self.yard_uid}.visualization'
        return None
    
    @property
    def yard_update_routing_key(self):
        """ Routing key value used to publish updates for the yard  """
        if self.yard_uid:
            return f'yard.{self.yard_uid}.update'
        return None

    @property
    def instant_actions_routing_key(self):
        """ Routing key value used to read instant actions  """

        return f'agent.{self.uuid}.instantActions'

    @property
    def update_routing_key(self):
        """ Routing key value used for agent update messages  """

        return f'agent.{self.uuid}.update'

    @property
    def assignment_routing_key(self):
        """ Routing key value used to read assigment messages  """

        return f'agent.{self.uuid}.assignment'

    def get_checkin_result(self):
        """ get_checkin_result() read the checkin data published by helyOS and save into the HelyOSClient instance
            as `checkin_data`.

         """

        self.tries = 0
        self.guest_channel.start_consuming()

    def auth_required(func):  # pylint: disable=no-self-argument
        @wraps(func)
        def wrap(*args, **kwargs):
            if not args[0].connection:
                raise HelyOSClientAutheticationError(
                    'HelyOSClient is not authenticated. Check the HelyosClient.perform_checkin() method.'
                )
            return func(*args, **kwargs)  # pylint: a disable=not-callable

        return wrap

    def __connect_as_anonymous(self):

        # step 1 - connect anonymously
        try:
            temp_connection = connect_rabbitmq(self.rabbitmq_host, self.rabbitmq_port,
                                               'anonymous', 'anonymous', 
                                               self.enable_ssl, 
                                               vhost=self.vhost, temporary=True)
            self.guest_channel = temp_connection.channel()
        except Exception as inst:
            print(inst)
            raise HelyOSAnonymousConnectionError(
                'Not able to connect as anonymous to rabbitMQ to perform check in.')

        # step 2 - creates a temporary queue to receive checkin response
        temp_queue = self.guest_channel.queue_declare(queue='', exclusive=True)
        self.checkin_response_queue = temp_queue.method.queue
        self.guest_channel.basic_consume(
            queue=self.checkin_response_queue, auto_ack=True, on_message_callback=self.__checkin_callback_wrapper)

    def __prepare_checkin_for_already_connected(self):
        # step 1 - use existent connection
        self.guest_channel = self.channel
        # step 2 - creates a temporary queue to receive checkin response
        temp_queue = self.guest_channel.queue_declare(queue='', exclusive=True)
        self.checkin_response_queue = temp_queue.method.queue
        self.guest_channel.basic_consume(
            queue=self.checkin_response_queue, auto_ack=True, on_message_callback=self.__checkin_callback_wrapper)

    def connect_rabbitmq(self, username, password):
        return self.connect(username, password)
    
    def reconnect(self):
        self.is_reconecting = True
        self.connect(self.rbmq_username, self.rbmq_password)
        self.is_reconecting = False


    def connect(self, username, password):
        """
        Creates the connection between agent and the RabbitMQ server.

        .. code-block:: python

            helyos_client = HelyOSClient(host='myrabbitmq.com', port=5672, uuid='3452345-52453-43525')
            helyos_client.connect_rabbitmq('my_username', 'secret_password') #  <===


        :param username:  username previously registered in RabbitMQ server
        :type username: str
        :param password: password previously registered in RabbitMQ server'
        :type password: str
        """
        print("connecting... ")
        try:
            self.connection = connect_rabbitmq(self.rabbitmq_host, self.rabbitmq_port, 
                                               username, password,
                                               self.enable_ssl, self.ca_certificate,
                                               vhost=self.vhost)
            self.channel = self.connection.channel()
            self.rbmq_username = username
            self.rbmq_password = password 
            print("connected")

        except Exception as inst:
            raise HelyOSAccountConnectionError(
                f'Not able to connect as {username} to rabbitMQ. {inst}')

    def perform_checkin(self, yard_uid, status=AGENT_STATE.FREE, agent_data={}, signed=False, checkin_guard_interceptor=None):
        """
        Registers the agent to a specific yard and retrieves relevant data about the yard and the CA certificate of the RabbitMQ server,
        which is relevant for SSL connections. Use the method `get_checkin_result()` to retrieve these data.

        The method `connect_rabbitmq()` should run before the check-in, otherwise, it will be assumed that the agent does not have yet a RabbitMQ account.
        In this case, if the environment variable REGISTRATION_TOKEN is set, helyOS will create a RabbitMQ account using the
        uuid as username and returns a password, which can be found in the property `rbmq_password`. This password should be safely stored.

        .. code-block:: python

            helyos_client = HelyOSClient(host='myrabbitmq.com', port=5672, uuid='3452345-52453-43525')
            helyos_client.connect_rabbitmq('my_username', 'secret_password')
            helyos_client.perform_checkin(yard_uid='yard_A', status='free')  #  <===
            helyOS_client.get_checkin_result()                               #  <===

        :param yard_uid: Yard UID
        :type yard_uid: str
        :param status: Agent status, defaults to 'free'
        :type status: str
        :param agent_data: Additional data to be sent with the check-in message, defaults to an empty dictionary
        :type agent_data: dict
        :param signed: Whether or not to sign the check-in message, defaults to False
        :type signed: bool
        :param checkin_guard_interceptor: An optional interceptor function to be called to validate the check-in response, returning True or False, defaults to None
        :type checkin_guard_interceptor: function
        """
        if self.connection:
            self.__prepare_checkin_for_already_connected()
            username = self.rbmq_username
        else:
            self.__connect_as_anonymous()
            username = 'anonymous'

        if checkin_guard_interceptor:
            self.checkin_guard_interceptor = checkin_guard_interceptor

        self.yard_uid = yard_uid
        checkin_msg = {'type': 'checkin',
                        'uuid': self.uuid,
                        'body': {'yard_uid': yard_uid,
                                'status': status,
                                'public_key': self.public_key.decode('utf-8'),
                                'public_key_format': 'PEM',
                                'registration_token': REGISTRATION_TOKEN,
                                **agent_data},
                        }
        
        message = json.dumps(checkin_msg, sort_keys=True)
        signature = None
        if signed:
            signature = self.signing_helper.return_signature(message).hex()

        body = json.dumps({'message': message, 'signature': signature}, sort_keys=True)

        self.guest_channel.basic_publish(exchange=AGENT_ANONYMOUS_EXCHANGE,
                                         routing_key=self.checking_routing_key,
                                         properties=pika.BasicProperties(
                                             reply_to=self.checkin_response_queue, user_id=username, timestamp=int(time.time()*1000)),
                                         body=body)

    def __checkin_callback_wrapper(self, channel, method, properties, received_str):
        try:
            self.__checkin_callback(channel, properties, received_str )
            channel.stop_consuming()
        except Exception as inst:
            self.tries += 1
            print(f'try {self.tries}')
            if self.tries > 3:
                channel.stop_consuming()

    def __checkin_callback(self, ch, properties, received_str):
        payload = json.loads(received_str)
        received_message_str = payload['message']
        signature = payload['signature']
        received_message = json.loads(received_message_str)
        sender = None
        if hasattr(properties, 'user_id'):
            sender = properties.user_id

        msg_type = received_message['type']
        if msg_type != 'checkin':
            print('waiting response...')
            return

        body = received_message['body']
        response_code = body.get('response_code', 500)
        if response_code != '200':
            print(body)
            message = body.get('message', 'Check in refused')
            raise HelyOSCheckinError(f'{message}: code {response_code}')

        try:
            checkin_data = CheckinResponseMessage(**received_message)
        except:
            raise HelyOSCheckinError('Check in refused: received invalid message format')

        if not self.checkin_guard_interceptor(ch, sender, checkin_data, received_message_str, signature):
            raise HelyOSCheckinError('Check in refused: checkin_guard_interceptor returned False')


        password = body.pop('rbmq_password', None)
        if self.ca_certificate is None:
            self.ca_certificate = body.get('ca_certificate', self.ca_certificate)
        if self.helyos_public_key is None:
            self.helyos_public_key = body.get('helyos_public_key', self.helyos_public_key)

        if password:
            self.connection = connect_rabbitmq(self.rabbitmq_host, self.rabbitmq_port,
                                               body['rbmq_username'], password, 
                                               self.enable_ssl, self.ca_certificate,
                                               vhost=self.vhost)
            self.channel = self.connection.channel()
            self.rbmq_username = body['rbmq_username']
            self.rbmq_password = password

            print('uuid', self.uuid)
            print('username', body['rbmq_username'])
            print('password', len(password)*'*')

        self.uuid = received_message['uuid']
        try:
            self.checkin_data = CheckinResponseMessage(**received_message)
        except:
            self.checkin_data = body

    @auth_required
    def publish(self, routing_key, message, signed=False, reply_to=None, corr_id=None, exchange=AGENTS_UL_EXCHANGE):
        """ Publish message in RabbitMQ
            :param message: Message to be transmitted
            :type message: str
            :param routing_key: RabbitMQ routing_key
            :type routing_key: str
            :param signed: If this message should be signed, defaults to False
            :type signed: boolean
            :param exchange: RabbitMQ exchange, defaults to env.AGENTS_UL_EXCHANGE
            :type exchange: str
        """

        if self.is_reconecting:
            return

        signature = None
        if signed:
            signature = self.signing_helper.return_signature(message).hex()
        

        headers = pika.BasicProperties( user_id=self.rbmq_username, 
                                        timestamp=int(time.time()*1000),
                                        reply_to=reply_to,
                                        correlation_id=corr_id)
        
        body = json.dumps({'message': message, 'signature': signature}, sort_keys=True)

        is_trying = True
        while is_trying:    
            try:
                self.channel.basic_publish(exchange, routing_key,
                                        properties=headers,
                                        body=body)
                is_trying = False
                
                
            except pika.exceptions.AMQPConnectionError as err:
                print(f"Connection error when publishing. Reconnecting... try {self.tries}")
                self.tries += 1                
                if self.tries > 3:
                    self.tries = 0
                    raise HelyOSAccountConnectionError("Connection error when publishing.")
                
                try: 
                    self.reconnect()
                    is_trying = False
                    self.tries = 0
                except Exception as err:
                    print(err)
                    is_trying = True
                time.sleep(3)  # Wait for a few seconds before reconnecting
                

    @auth_required
    def set_assignment_queue(self, exchange=AGENTS_DL_EXCHANGE):
        self.assignment_queue = self.channel.queue_declare(queue='')
        self.channel.queue_bind(queue=self.assignment_queue.method.queue,
                                exchange=exchange, routing_key=self.assignment_routing_key)
        return self.assignment_queue

    @auth_required
    def set_instant_actions_queue(self, exchange=AGENTS_DL_EXCHANGE):
        self.instant_actions_queue = self.channel.queue_declare(queue='')
        self.channel.queue_bind(queue=self.instant_actions_queue.method.queue,
                                exchange=exchange, routing_key=self.instant_actions_routing_key)
        return self.instant_actions_queue

    @auth_required
    def consume_assignment_messages(self, assignment_callback):
        self.set_assignment_queue()
        self.channel.basic_consume(queue=self.assignment_queue.method.queue, auto_ack=True,
                                   on_message_callback=assignment_callback)

    @auth_required
    def consume_instant_actions_messages(self, instant_actions_callback):
        """ Receive instant actions messages.
            Instant actions are used by helyOS to reserve, release or cancel an assignment.

            :param instant_actions_callback: call back for instant actions
            :type instant_actions_callback: func

        """

        self.set_instant_actions_queue()
        self.channel.basic_consume(queue=self.instant_actions_queue.method.queue, auto_ack=True,
                                   on_message_callback=instant_actions_callback)

    def start_listening(self):
        self.channel.start_consuming()

    def stop_listening(self):
        """ Stop the AMQP connection with RabbitMQ server
            For multi-threaded environments, use the method stop_listening_threadsafe()
        """
        self.channel.stop_consuming()

    def stop_listening_threadsafe(self):
        """ Stop the AMQP connection with RabbitMQ server
            This method should be used in a multi-threaded environment.
            Otherwise, use the method stop_listening()
        """
        self.connection.add_callback_threadsafe(self.channel.stop_consuming)

    def close_connection(self):
        """ Close the AMQP connection with RabbitMQ server """
        self.connection.close()
