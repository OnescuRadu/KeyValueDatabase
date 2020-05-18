import socket
import pickle
import operator
from request import Request
from response import Response
from apscheduler.schedulers.background import BackgroundScheduler
from configparser import ConfigParser
from pyparsing import Keyword, Word, Literal, printables


class Server():
    """
    This is a class for using and initializing a key-value pair database that can be accesed over the network.
    It communicates with the client using Request and Response objects sent and received via TCP Sockets.
    It supports only one connection at a time.

    Functionalities:
        - Initializes a key-value pair database based on the given filename in the config file. (default filename: data)
        - Initializes a TCP Socket Server bound to the given host and port in the config file. (default hostname and port: 127.0.0.1:65535)
        - On initialization it reads the database file if it exists, else if there is no backup of the database, it will create a new one.
        - Read, add, delete, query for the database.
        - Creates a snapshot of the data (the key-value pair dictionary) at a given time based on the interval value from the config file. (default: every 60 mins)

    Attributes:
        data (dict): The dictionary object that acts a database.
        host (String): The host on which the server is bound.
        port (Int): The port on which the server is bound.
        filename (String): The database file name.
        snapshot_interval (Int): The interval on which the snapshot is created.
        server_socket: The server's TCP Socket.

    Tests:
    >>> Server()
        Traceback (most recent call last):
        ...
        ConnectionError: Server could not be started.

    """

    def __init__(self):
        """
        The constructor for the database's server class.
        """
        self._read_config()
        self._init_db()
        self._schedule_snapshot()
        self._start_server()
        self._listen()

    def _init_db(self):
        """
        The method tries to open the file with the given name and to add the containing data to the database.
        If there is no file or the data is corrupted it will initilize a new data object.
        """
        try:
            with open(self.filename, 'rb') as handle:
                self.data = pickle.loads(handle.read())
        except IOError:
            self.data = {}

    def _start_server(self):
        """
        The method creates a socket bounded to the given host and port and listens for an incoming client connection.

        Raises:
            ConnectionError: Server could not be started.
        """
        try:
            self.server_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(1)
            print(f"Started server on {self.host}:{self.port}")
        except:
            raise ConnectionError("Server could not be started.")

    def _listen(self):
        """
        The method accepts the incoming client connection and permanently listens for any incoming request from the client.
        When a request is received, it checks its type, calls the corresponding action and then sends back the response to the client.
        When the client disconnects, it waits for other incoming client connections.
        """
        while True:
            # Starting the connection
            client_socket, address = self.server_socket.accept()

            while True:
                request = client_socket.recv(1024)

                if request:
                    request = pickle.loads(request)

                    request_types = {
                        0: lambda: self._read(request.key),
                        1: lambda: self._add(request.key, request.value),
                        2: lambda: self._delete(request.key),
                        3: lambda: self._query(request.query),
                        4: lambda: self._send_error("Request type does not exist.")}
                    response = request_types.get(request.request_type, 4)()
                    response = pickle.dumps(response)
                    client_socket.send(response)
                else:  # client disconnected
                    break

    def _read(self, key):
        """
        The method reads an entry (Key-Value pair), from the database, based on the given key.

        Parameters:
            key (Any hashable data type): The key of the entry.

        Returns:
            Response(success=True, message=None, data=[(...)]): If the read action was succesful. Note: The data list will contain the read entry.
            Response(success=False, message=Entry does not exist., data=[]): If the entry does not exist.
        """
        try:
            return Response(True, None, [(key, self.data[key])])
        except:
            return self._send_error("Entry does not exist.")

    def _add(self, key, value):
        """
        The method adds an entry (Key-Value pair) to the database.

        Parameters:
            key (Any hashable data type): The key of the entry.
            value (Any data type): The value of the entry.

        Returns:
            Response(success=True, message=None, data=[(...)]): If the add action was succesful. Note: The data list will contain the added entry.
            Response(success=False, message=Entry could not be added., data=[]): If the add action was not succesful.
        """
        try:
            self.data[key] = value
            return Response(True, None, [(key, self.data[key])])
        except:
            return self._send_error("Entry could not be added.")

    def _delete(self, key):
        """
        The method deletes an entry (Key-Value pair), from the database, based on the given key.

        Parameters:
            key (Any hashable data type): The key of the entry that will be deleted.

        Returns:
            Response(success=True, message=None, data=[(...)]): If the delete action was succesful.
            Response(success=False, message=Entry could not be deleted., data=[]): If the delete action was not succesful.
        """
        try:
            del self.data[key]
            return Response(True, None, [])
        except:
            return self._send_error("Entry could not be deleted.")

    def _query(self, query):
        """
        The method queries the database based on the given query.

        Parameters:
            query (String): The query string.

        Returns:
            Response(success=True, message=None, data=[(...)]): If the query action was succesful. Note: The data list will contain the entries matching the query.
            Response(success=False, message=Invalid query syntax., data=[]): If the query action was not succesful.
        """
        try:
            query = self._parse_query_string(query)

            operators = {
                ">": operator.gt,
                "<": operator.lt,
                "=": operator.eq,
                "<=": operator.le,
                ">=": operator.ge,
                "contains": operator.contains,
            }

            query_elements = {
                "key": self._execute_query_by_key,
                "value": self._execute_query_by_value,
            }

            query_actions = {
                "read": None,
                "delete": self._delete_from_query
            }

            query_action = query_actions[query["action"]]
            matches = query_elements[query["element"]](
                query_action, query, operators)

            return Response(True, None, matches)
        except:
            return self._send_error("Invalid query syntax.")

    def _parse_query_string(self, query):
        """
        The method checks the format and parses the given query.

        Parameters:
            query (String): The query string.
                            Accepted formats: "[ACTION] [ELEMENT] [OPERATOR] [VALUE]" or "[ACTION] [ELEMENT] [OPERATOR] [DATATYPE] ( [VALUE] )"
                            [ACTION] can be "read" or "delete"
                            [ELEMENT] can be "value" or "key"
                            [OPERATOR] can be "<", ">", "=", "<=", ">=", "contains"
                            [DATATYPE] can be "int", "float", "complex", "str".
                            [VALUE] is the value on which the query will be done.
                            Examples: "read key > 1234"
                                      "read value < int ( 4 )"
                            Note: If no datatype is provided, the value will have the String data type by default.

        Returns:
           query (List): A list containing the parsed query string.
        """
        ACTION = Keyword("read")("action") | Keyword("delete")("action")

        ELEMENT = Keyword("key")("element") | Keyword("value")("element")

        OPERATOR = Keyword("<=")("operator") | Keyword(">=")("operator") | Keyword("<")("operator") | Keyword(">")("operator") | Keyword(
            "=")("operator") | Keyword("contains")("operator")

        VALUE = (Word(printables)("value_type") +
                 "(" + Word(printables)("value") + ")") | Word(printables)("value")

        SYNTAX = ACTION + ELEMENT + OPERATOR + VALUE

        query = SYNTAX.parseString(query)
        if "value_type" in query:
            query["value"] = self._parse_value_to_type(
                query["value"], query["value_type"])

        return query

    def _parse_value_to_type(self, value, value_type):
        """
        The method casts the given value to the given type.

        Parameters:
            value (String): The value.
            value_type (String): The type to which the value will be casted to.

        Returns:
           The value casted to the given type.
        """
        types = {
            "int": int,
            "float": float,
            "complex": complex,
            "str": str
        }
        return types[value_type](value)

    def _execute_query_by_value(self, query_action, query, operators):
        """
        The method queries the database by value using the given query.

        Parameters:
            query_action (Method): The query action that will be executed. Note: If the query action is "read" the parameter will be None, because the method reads the matching data in any case.
            query (List): A list containing the parsed query string.
            operators (Dict): A dictionary containing the operators that will be used.

        Returns:
           A list containing all the entries that match the query.
        """
        matches = []
        for key, value in self.data.copy().items():
            try:
                if operators[query["operator"]](value, query["value"]):
                    matches.append((key, value))
                    query_action(key)
            except:
                pass
        return matches

    def _execute_query_by_key(self, query_action, query, operators):
        """
        The method queries the database by key using the given query.
        Note: If the query operator is "=" the method will access the entry, using the key, right away.

        Parameters:
            query_action (Method): The query action that will be executed. Note: If the query action is "read" the parameter will be None, because the method reads the matching data in any case.
            query (List): A list containing the parsed query string.
            operators (Dict): A dictionary containing the operators that will be used.

        Returns:
           A list containing all the entries that match the query.
        """
        matches = []
        if query["operator"] == "=":
            try:
                matches.append((query["value"], self.data[query["value"]]))
                query_action(query["value"])
            except:
                pass
            return matches
        else:
            for key, value in self.data.copy().items():
                try:
                    if operators[query["operator"]](key, query["value"]):
                        matches.append((key, value))
                        query_action(key)
                except:
                    pass
        return matches

    def _delete_from_query(self, key):
        """
        The method deletes an entry from the database using the given key.

        Parameters:
            key: The key of the database entry.
        """
        del self.data[key]

    def _send_error(self, description):
        """
        The method creates an error Response object based on the given description.

        Parameters:
            description (String): The error's description.

        Returns:
            Response(False, "The given description", [])
        """
        return Response(False, description, [])

    def _create_snapshot(self):
        """
        The method creates a snapshot (a backup) of the database.

        Raises:
            PermissionError: Permission denied to write to file.
        """
        try:
            with open(self.filename, 'wb') as handle:
                pickle.dump(self.data, handle)
        except:
            raise PermissionError("Permission denied to write to file.")

    def _schedule_snapshot(self):
        """
        The method creates a background thread that will call the _create_snapshot() method at a given time interval.
        """
        scheduler = BackgroundScheduler()
        scheduler.add_job(self._create_snapshot, 'interval',
                          minutes=self.snapshot_interval)
        scheduler.start()

    def _read_config(self):
        """
        The method reads the data from the config file.
        If there is no 'config.ini' file or it is corrupted, it will assign the default values.
        """
        try:
            config = ConfigParser()
            config.read('config.ini')
            self.host = config.get("database", "host")
            self.port = int(config.get("database", "port"))
            self.filename = config.get("database", "filename")
            self.snapshot_interval = int(config.get("snapshot", "interval"))
        except:
            self.host = "127.0.0.1"
            self.port = 65535
            self.filename = "data"
            self.snapshot_interval = 60
