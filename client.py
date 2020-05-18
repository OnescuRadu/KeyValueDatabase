import socket
import pickle
from request import Request
from response import Response


class Client:
    """
    This is a class for connecting to the server's database and to read, add, delete or query entries (key-value pairs).
    It establishes a TCP connection with the server.
    It communicates with the server using Request and Response objects sent and received via TCP Sockets.

    Note: In order for tests to work the server should be running on 127.0.0.1:65534
    Tests:
        >>> client = Client("127.0.0.1", 65535)
        Traceback (most recent call last):
        ...
        ConnectionRefusedError: Connection to the server was refused.
        >>> client = Client("127.0.0.1", 65534)
        >>> client.add(1, 123)
        Response(success=True, message=None, data=[(1, 123)])
        >>> client.add("1a2b3c", "John")
        Response(success=True, message=None, data=[('1a2b3c', 'John')])
        >>> client.add(10, "Radu-Mihai")
        Response(success=True, message=None, data=[(10, 'Radu-Mihai')])
        >>> client.add(15, "Onescu")
        Response(success=True, message=None, data=[(15, 'Onescu')])
        >>> client.read(1)
        Response(success=True, message=None, data=[(1, 123)])
        >>> client.read(2)
        Response(success=False, message=Entry does not exist., data=[])
        >>> client.delete(1)
        Response(success=True, message=None, data=[])
        >>> client.delete(2)
        Response(success=False, message=Entry could not be deleted., data=[])
        >>> client.query("read value = John")
        Response(success=True, message=None, data=[('1a2b3c', 'John')])
        >>> client.query("read key > int ( 5 )")
        Response(success=True, message=None, data=[(15, 'Onescu'), (10, 'Radu-Mihai')])
        >>> client.query("read value contains Mihai")
        Response(success=True, message=None, data=[(10, 'Radu-Mihai')])
        >>> client.query("delete value contains Mihai")
        Response(success=True, message=None, data=[(10, 'Radu-Mihai')])
        >>> client.query("read value contains Mihai")
        Response(success=True, message=None, data=[])
        >>> client.query("read something > int ( 5 )")
        Response(success=False, message=Invalid query syntax., data=[])
    """

    def __init__(self, host, port):
        """
        The constructor for the database's client class.

        Parameters:
           host (String): The host of the server that is trying to establish a connection to.
           port (int): The port on which the server is bound.
        """
        self._connect_to_server(host, port)

    def read(self, key):
        """
        The method reads an entry (Key-Value pair), from the database, based on the given key.
        It sends a request to the server and waits for the response.

        Parameters:
            key (Any hashable data type): The key of the entry.

        Returns:
            Response(success=True, message=None, data=[(...)]): If the read action was succesful. Note: The data list will contain the read entry.
            Response(success=False, message=Entry does not exist., data=[]): If the entry does not exist.
        """
        request = Request(0, key, None, None)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def add(self, key, value):
        """
        The method adds an entry (Key-Value pair) to the database.
        It sends a request to the server and waits for the response.

        Parameters:
            key (Any hashable data type): The key of the entry.
            value (Any data type): The value of the entry.

        Returns:
            Response(success=True, message=None, data=[(...)]): If the add action was succesful. Note: The data list will contain the added entry.
            Response(success=False, message=Entry could not be added., data=[]): If the add action was not succesful.
        """
        request = Request(1, key, value, None)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def delete(self, key):
        """
        The method deletes an entry (Key-Value pair), from the database, based on the given key.
        It sends a request to the server and waits for the response.

        Parameters:
            key (Any hashable data type): The key of the entry that will be deleted.

        Returns:
            Response(success=True, message=None, data=[(...)]): If the delete action was succesful.
            Response(success=False, message=Entry could not be deleted., data=[]): If the delete action was not succesful.
        """
        request = Request(2, key, None, None)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def query(self, query):
        """
        The method queries the database based on the given query.
        It sends a request to the server and waits for the response.

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
            Response(success=True, message=None, data=[(...)]): If the query action was succesful. Note: The data list will contain the entries matching the query.
            Response(success=False, message=Invalid query syntax., data=[]): If the query action was not succesful.
        """
        request = Request(3, None, None, query)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def _listen_for_response(self):
        """
        The method waits and listens for a response from the server.

        Returns: 
            Response: When the response has been received from the server. 
        """
        while True:
            response = self.client_socket.recv(1024)
            if response:
                response = pickle.loads(response)
                return response

    def _connect_to_server(self, host, port):
        """
        The method connects to the server using the given host and port. 

        Parameters: 
           host (String): The host of the server that is trying to establish a connection to.
           port (int): The port on which the server is bound. 

        Raises:
            ConnectionRefusedError: Connection to the server was refused.
        """
        try:
            self.client_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((host, port))
        except:
            raise ConnectionRefusedError(
                "Connection to the server was refused.")


if __name__ == "__main__":
    import doctest
    doctest.testmod()
