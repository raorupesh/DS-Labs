"""
CPSC 5520, Seattle University
Assignment Name: Dynamic Hash Table (DHT)
Author: Rupeshwar Rao
"""

import hashlib
import socket
import pickle
import sys

BUFFER_SIZE = 4096  # Define the buffer size for receiving data

def hash_key(key: str) -> int:
    """
    Hashes the given key using SHA-1 and returns an integer representation of the hash.

    Args:
        key (str): The key to be hashed.

    Returns:
        int: The integer representation of the SHA-1 hash.
    """
    sha1 = hashlib.sha1()
    sha1.update(key.encode('utf-8'))
    return int(sha1.hexdigest(), 16)

def call_rpc(server_address: tuple[str, int], method_name: str, *args) -> any:
    """
    Calls a remote procedure via socket connection to a given server address.

    Args:
        server_address (tuple): The (host, port) tuple for the server to connect to.
        method_name (str): The name of the RPC method to call.
        *args: The arguments to pass to the RPC method.

    Returns:
        any: The result of the RPC call, or None in case of failure.
    """
    try:
        # Create and connect the socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(server_address)

            # Serialize the method call
            request = pickle.dumps((method_name, args))
            sock.sendall(request)

            # Receive the response in chunks
            response = b''
            while True:
                data = sock.recv(BUFFER_SIZE)
                if not data:
                    break
                response += data
                if len(data) < BUFFER_SIZE:
                    break

            # Deserialize the response
            result = pickle.loads(response)
            return result
    except Exception as e:
        print(f'Error calling RPC {method_name} on {server_address}: {e}')
        return None

def main() -> None:
    """
    Main entry point of the script to perform a Chord lookup RPC.

    Expects two command line arguments:
    - The port number of the known node to connect to.
    - The key to lookup.

    Prints the value associated with the key, or an error message if not found.
    """
    if len(sys.argv) != 3:
        print('Usage: python chord_query.py <known_port> <key>')
        sys.exit(1)

    known_port = int(sys.argv[1])
    key = sys.argv[2]
    server_address = ('localhost', known_port)

    # Call the 'lookup' RPC method
    result = call_rpc(server_address, 'lookup', key)

    if result:
        print(f'Value for key "{key}":')
        print(result)
    else:
        print(f'Key "{key}" not found.')

if __name__ == '__main__':
    main()
