"""
CPSC 5520, Seattle University
Assignment Name: Dynamic Hash Table (DHT)
Author: Rupeshwar Rao
"""

import sys
import hashlib
import socket
import pickle
import csv

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

def populate_data(known_port: int, data_file: str, num_rows: int = None) -> None:
    """
    Reads a CSV data file and populates a distributed key-value store with data from the file.
    The key is a combination of 'Player Id' and 'Year', and the value is the entire row.

    Args:
        known_port (int): The port of the known node to connect to for RPC calls.
        data_file (str): The CSV file containing the data to be populated.
        num_rows (int, optional): The number of rows to process from the CSV file. 
                                  If None, processes all rows.
    """
    server_address = ('localhost', known_port)
    try:
        with open(data_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows_processed = 0
            for row in reader:
                # Ensure the row contains the required fields
                if 'Player Id' in row and 'Year' in row:
                    key = row['Player Id'] + row['Year']
                    value = row
                    print(f'Populating key "{key}"')
                    call_rpc(server_address, 'store', key, value)
                    rows_processed += 1
                    if num_rows and rows_processed >= num_rows:
                        break
                else:
                    print(f'Row missing Player Id or Year: {row}')
        print(f'Data population completed. Rows processed: {rows_processed}')
    except Exception as e:
        print(f'Error reading data file: {e}')

def main() -> None:
    """
    Main entry point of the script to populate the key-value store with data from a CSV file.
    Expects the following command-line arguments:
    - The port of the known node to connect to.
    - The CSV data file to read from.
    - Optionally, the number of rows to process from the file.
    """
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print('Usage: python chord_populate.py <known_port> <data_file> [num_rows]')
        sys.exit(1)

    known_port = int(sys.argv[1])
    data_file = sys.argv[2]
    num_rows = None

    # Parse the optional num_rows argument
    if len(sys.argv) == 4:
        try:
            num_rows = int(sys.argv[3])
            if num_rows < 1:
                raise ValueError
        except ValueError:
            print('Invalid number of rows specified.')
            sys.exit(1)

    # Populate the data using the given arguments
    populate_data(known_port, data_file, num_rows)

if __name__ == '__main__':
    main()
