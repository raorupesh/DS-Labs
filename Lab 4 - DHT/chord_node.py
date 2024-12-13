"""
CPSC 5520, Seattle University
Assignment Name: Dynamic Hash Table (DHT)
Author: Rupeshwar Rao
"""

import threading
import socket
import pickle
import hashlib
import sys
import time
import random

M = 6  # Number of bits for the identifier space
NODES = 2 ** M
BUFFER_SIZE = 4096  # socket recv arg
BACKLOG = 10   # socket listen arg
INTERVALS = 10  # seconds (reduced for faster stabilization during testing)

class ModRange(object):
    """ 
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.
    
    Attributes:
        start (int): The start of the range.
        stop (int): The end of the range.
        divisor (int): The divisor for modulo arithmetic.
        intervals (tuple): Tuple of range objects representing the intervals.
    """

    def __init__(self, start, stop, divisor):
        """
        Initialize the ModRange object.

        Args:
            start (int): The start of the range.
            stop (int): The end of the range.
            divisor (int): The divisor for modulo arithmetic.
        """
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.start == self.stop:
            self.intervals = range(0)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __contains__(self, id):
        """
        Check if an id is within the ModRange.

        Args:
            id (int): The id to check.

        Returns:
            bool: True if id is within the range, False otherwise.
        """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

class FingerEntry(object):
    """ 
    Row in a finger table.
    
    Attributes:
        start (int): The start of the interval.
        interval (ModRange): The interval for which this finger is responsible.
        node (tuple): The node (node_id, address) this finger points to.
    """

    def __init__(self, n, k, node=None):
        """
        Initialize a FingerEntry.

        Args:
            n (int): The node identifier.
            k (int): The finger table index.
            node (tuple): The node (node_id, address) this finger points to.
        
        Raises:
            ValueError: If the node identifier or finger table index is invalid.
        """
        if not (0 <= n < NODES and 0 <= k <= M):
            raise ValueError('Invalid node or finger table index')
        self.start = (n + 2 ** (k - 1)) % NODES if k < M else n
        self.interval = ModRange(self.start, (n + 2 ** k) % NODES, NODES)
        self.node = node  # (node_id, address)

    def __contains__(self, id):
        """
        Check if an id is within the interval of this finger.

        Args:
            id (int): The id to verify.

        Returns:
            bool: True if id is within the interval, False otherwise.
        """
        return id in self.interval

class ChordNode(object):
    """
    Represents a node in the Chord Distributed Hash Table (DHT) network.
    
    Attributes:
        ip (str): The IP address of the node.
        port (int): The port number of the node.
        address (tuple): The address of the node (ip, port).
        id (int): The identifier of the node.
        predecessor (tuple): The predecessor node (node_id, address).
        finger (list): The finger table of the node.
        data (dict): The key-value store of the node.
        stop_event (threading.Event): Event to signal the node to stop.
    """

    def __init__(self, known_port):
        """
        Initialize a ChordNode.

        Args:
            known_port (int): The port of a known node in the network. Use 0 to start a new network.
        """
        self.ip = 'localhost'
        self.port = self.bind_socket()
        self.address = (self.ip, self.port)
        self.id = self.hash(str(self.address))
        self.predecessor = None  # (node_id, address)
        self.finger = [None]  # Finger table index starts at 1
        for i in range(1, M + 1):
            self.finger.append(FingerEntry(self.id, i, (self.id, self.address)))
        self.data = {}  # Key-value store
        self.stop_event = threading.Event()

        # Start the server thread first (set as daemon)
        threading.Thread(target=self.run_server, daemon=True).start()
        # Start the stabilize thread (set as daemon)
        threading.Thread(target=self.stabilize_loop, daemon=True).start()

        time.sleep(1)  # Ensure the server is ready

        if known_port == 0:
            # Start a new network
            self.predecessor = None
            self.finger[1].node = (self.id, self.address)
            print(f'\n[Node {self.id}] [Node Initialization Details] Starting a new network.')
        else:
            # Join an existing network
            known_address = ('localhost', known_port)
            self.join(known_address)
            print(f'\n[Node {self.id}] [Node Initialization Details] Joining the network via node at port {known_port}.')

        # Print the initial finger table
        self.print_finger_table()

    @staticmethod
    def hash(key):
        """
        Generate a hash for the given key.

        Args:
            key (str): The key to hash.

        Returns:
            int: The hash value of the key.
        """
        sha1 = hashlib.sha1()
        sha1.update(key.encode('utf-8'))
        return int(sha1.hexdigest(), 16) % NODES

    def bind_socket(self):
        """
        Bind a socket to an available port.

        Returns:
            int: The port number the socket is bound to.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))  # Bind to any available port
        port = sock.getsockname()[1]
        sock.close()
        return port

    def run_server(self):
        """
        Run the server to handle incoming RPC requests.
        """
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((self.ip, self.port))
        self.server.listen(BACKLOG)
        print(f'\n[Node {self.id}] [Network Details] Listening on {self.ip}:{self.port}')
        self.server.settimeout(1.0)
        while not self.stop_event.is_set():
            try:
                client_sock, _ = self.server.accept()
                client_sock.settimeout(2.0)
                threading.Thread(target=self.handle_rpc, args=(client_sock,), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f'\n[Node {self.id}] [Error Details] Error in server loop: {e}')
                break
        self.server.close()

    def handle_rpc(self, client_sock):
        try:
            client_sock.settimeout(2.0)
            rpc = b''
            while True:
                data = client_sock.recv(BUFFER_SIZE)
                if not data:
                    break
                rpc += data
                if len(data) < BUFFER_SIZE:
                    break
            method_name, args = pickle.loads(rpc)
            method = getattr(self, method_name)
            result = method(*args)
            client_sock.sendall(pickle.dumps(result))
        except Exception as e:
            if not self.stop_event.is_set():
                print(f'\n[Node {self.id}] [Error Details] Error handling RPC: {e}')
        finally:
            client_sock.close()

    def call_rpc(self, address, method_name, *args):
        """
        Call a remote procedure on another node.

        Args:
            address (tuple): The address of the node to call.
            method_name (str): The name of the method to call.
            *args: The arguments to pass to the method.

        Returns:
            any: The result of the remote procedure call, or None if an error occurred.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect(address)
            request = pickle.dumps((method_name, args))
            sock.sendall(request)
            response = b''
            while True:
                data = sock.recv(BUFFER_SIZE)
                if not data:
                    break
                response += data
                if len(data) < BUFFER_SIZE:
                    break
            result = pickle.loads(response)
            sock.close()
            return result
        except Exception as e:
            if not self.stop_event.is_set():
                print(f'\n[Node {self.id}] [Error Details] Error calling RPC {method_name} on {address}: {e}')
            return None

    def join(self, known_address):
        """
        Join an existing Chord network.

        Args:
            known_address (tuple): The address of a known node in the network.
        """
        if known_address == self.address:
            # Starting new network
            self.predecessor = None
            self.finger[1].node = (self.id, self.address)
        else:
            print(f'\n[Node {self.id}] [Node Initialization Details] Attempting to join network via {known_address}')
            self.init_finger_table(known_address)
            self.update_others()
            # Move keys from successor
            successor_id, successor_addr = self.finger[1].node
            data = self.call_rpc(successor_addr, 'transfer_keys', self.id)
            if data:
                self.data.update(data)
            print(f'\n[Node {self.id}] [Node Initialization Details] Joined the network successfully.')

        # Print the updated finger table after joining
        self.print_finger_table()

    def init_finger_table(self, known_address):
        """
        Initialize the finger table of the local node.

        Args:
            known_address (tuple): The address of a known node in the network.
        """
        # Find the successor for the first finger table entry
        s = self.call_rpc(known_address, 'find_successor', self.finger[1].start)
        self.finger[1].node = s
        successor_id, successor_addr = self.finger[1].node

        # Set the predecessor of the local node
        self.predecessor = self.call_rpc(successor_addr, 'get_predecessor')
        self.call_rpc(successor_addr, 'set_predecessor', (self.id, self.address))
        print(f'\n[Node {self.id}] [Finger Table Details] Initialized finger table with successor {successor_id}')

        # Initialize the rest of the finger table
        for i in range(1, M):
            if self.finger[i + 1].start in ModRange(self.id, self.finger[i].node[0], NODES):
                self.finger[i + 1].node = self.finger[i].node
            else:
                s = self.call_rpc(known_address, 'find_successor', self.finger[i + 1].start)
                self.finger[i + 1].node = s
        print(f'\n[Node {self.id}] [Finger Table Details] Finger table initialized.')

    def update_others(self):
        """
        Update the finger tables of other nodes in the network.
        """
        print(f'\n[Node {self.id}] [Finger Table Update Details] Updating other nodes\' finger tables.')
        updated_nodes = set()
        for i in range(1, M + 1):
            pred_id = (self.id - 2 ** (i - 1) + NODES) % NODES
            p = self.find_predecessor(pred_id)
            p_id, p_addr = p
            if p_id != self.id and p_id not in updated_nodes:
                self.call_rpc(p_addr, 'update_finger_table', (self.id, self.address), i)
                updated_nodes.add(p_id)
        if updated_nodes:
            print(f'\n[Node {self.id}] [Finger Table Update Details] Notified nodes to update finger tables: {sorted(updated_nodes)}')

    def update_finger_table(self, s, i):
        """
        Update the finger table entry at index i with node s.

        Args:
            s (tuple): The node (node_id, address) to update the finger table with.
            i (int): The index of the finger table entry to update.
        """
        s_id, s_addr = s
        if s_id != self.id and s_id in ModRange(self.finger[i].start, self.finger[i].node[0], NODES):
            self.finger[i].node = s
            # Collect updated indices
            if not hasattr(self, 'updated_indices'):
                self.updated_indices = set()
            self.updated_indices.add(i)
            p = self.predecessor
            if p and p != (self.id, self.address):
                p_id, p_addr = p
                self.call_rpc(p_addr, 'update_finger_table', s, i)
            # After all updates, print the updated finger table
            if self.predecessor == (s_id, s_addr):
                self.print_updated_finger_table()

    def print_updated_finger_table(self):
        """
        Print the updated finger table entries.
        """
        print(f'\n[Node {self.id}] [Finger Table Update Details] Updated Finger Table:')
        for i in sorted(self.updated_indices):
            entry = self.finger[i]
            print(f'  Start: {entry.start}, Interval: {entry.interval.intervals}, Node: {entry.node[0]}')
        self.updated_indices.clear()

    def find_successor(self, id):
        """
        Find the successor of the given id.

        Args:
            id (int): The identifier to find the successor for.

        Returns:
            tuple: The successor node (node_id, address).
        """
        if id == self.id:
            return (self.id, self.address)
        else:
            pred = self.find_predecessor(id)
            pred_id, pred_addr = pred
            return self.call_rpc(pred_addr, 'get_successor')

    def find_predecessor(self, id):
        """
        Find the predecessor of the given id.

        Args:
            id (int): The identifier to find the predecessor for.

        Returns:
            tuple: The predecessor node (node_id, address).
        """
        n_id, n_addr = (self.id, self.address)
        n_successor = self.finger[1].node
        while not id in ModRange(n_id + 1, n_successor[0] + 1, NODES):
            n_closest = self.closest_preceding_finger(id)
            if n_closest == (n_id, n_addr):
                break
            n_id, n_addr = n_closest
            n_successor = self.call_rpc(n_addr, 'get_successor')
        return (n_id, n_addr)

    def closest_preceding_finger(self, id):
        """
        Find the closest preceding finger for the given id.

        Args:
            id (int): The identifier to find the closest preceding finger for.

        Returns:
            tuple: The closest preceding finger node (node_id, address).
        """
        for i in range(M, 0, -1):
            finger_id, finger_addr = self.finger[i].node
            if finger_id != self.id and finger_id in ModRange(self.id + 1, id, NODES):
                # Collect closest preceding fingers
                if not hasattr(self, 'closest_fingers'):
                    self.closest_fingers = set()
                self.closest_fingers.add(finger_id)
                return self.finger[i].node
        return (self.id, self.address)

    def get_successor(self):
        """
        Get the successor of the current node.

        Returns:
            tuple: The successor node (node_id, address).
        """
        return self.finger[1].node

    def get_predecessor(self):
        """
        Get the predecessor of the current node.

        Returns:
            tuple: The predecessor node (node_id, address).
        """
        return self.predecessor

    def set_predecessor(self, pred):
        """
        Set the predecessor of the current node.

        Args:
            pred (tuple): The predecessor node (node_id, address).
        """
        self.predecessor = pred
        if pred:
            print(f'\n[Node {self.id}] [Predecessor Details] Predecessor updated to node {pred[0]}')
        else:
            print(f'\n[Node {self.id}] [Predecessor Details] Predecessor set to None')

    def stabilize(self):
        """
        Perform stabilization to ensure the node's successor is correct.
        """
        successor_id, successor_addr = self.finger[1].node
        x = self.call_rpc(successor_addr, 'get_predecessor')
        if x:
            x_id, x_addr = x
            if x_id != self.id and x_id in ModRange(self.id + 1, successor_id, NODES):
                print(f'\n[Node {self.id}] [Successor Details] Updating successor from {successor_id} to {x_id}')
                self.finger[1].node = x
        self.call_rpc(self.finger[1].node[1], 'notify', (self.id, self.address))

    def notify(self, n):
        """
        Notify the node about a potential predecessor.

        Args:
            n (tuple): The notifying node (node_id, address).
        """
        if not self.predecessor or n[0] in ModRange(self.predecessor[0] + 1, self.id, NODES):
            self.predecessor = n
            print(f'\n[Node {self.id}] [Predecessor Details] Notified by node {n[0]}. Predecessor updated.')

    def stabilize_loop(self):
        """
        Periodically run the stabilize function to maintain the network.
        """
        while not self.stop_event.is_set():
            self.stabilize()
            time.sleep(INTERVALS)

    def transfer_keys(self, new_node_id):
        """
        Transfer keys to a new node that is joining the network.

        Args:
            new_node_id (int): The identifier of the new node.

        Returns:
            dict: The keys and values to be transferred.
        """
        transferred_data = {}
        for key in list(self.data.keys()):
            key_id = self.hash(key)
            # The successor transfers keys for which the new node is now responsible
            if key_id in ModRange(self.id + 1, new_node_id + 1, NODES):
                transferred_data[key] = self.data.pop(key)
                print(f'\n[Node {self.id}] [Transfer Key Details] Transferring key "{key}" to node {new_node_id}')
        return transferred_data

    def store(self, key, value):
        """
        Store a key-value pair in the DHT.

        Args:
            key (str): The key to store.
            value (any): The value to store.

        Returns:
            bool: True if the key-value pair is stored locally, False otherwise.
        """
        key_id = self.hash(key)
        if self.predecessor:
            if key_id in ModRange(self.predecessor[0] + 1, self.id + 1, NODES):
                self.data[key] = value
                print(f'\n[Node {self.id}] [DHT Details] Stored key "{key}" locally.')
                return True
        else:
            # If predecessor is None (only node in the network)
            self.data[key] = value
            print(f'\n[Node {self.id}] [DHT Details] Stored key "{key}" locally.')
            return True

        successor_id, successor_addr = self.find_successor(key_id)
        print(f'\n[Node {self.id}] [DHT Details] Forwarding store request for key "{key}" to node {successor_id}')
        return self.call_rpc(successor_addr, 'store', key, value)

    def lookup(self, key):
        """
        Lookup a key in the DHT.

        Args:
            key (str): The key to lookup.

        Returns:
            any: The value associated with the key, or None if not found.
        """
        key_id = self.hash(key)
        if self.predecessor:
            if key_id in ModRange(self.predecessor[0] + 1, self.id + 1, NODES):
                print(f'\n[Node {self.id}] [Query Request Details] Key "{key}" found locally.')
                return self.data.get(key, None)
        else:
            # If predecessor is None (only node in the network)
            print(f'\n[Node {self.id}] [Query Request Details] Key "{key}" found locally.')
            return self.data.get(key, None)

        successor_id, successor_addr = self.find_successor(key_id)
        print(f'\n[Node {self.id}] [Query Request Details] Forwarding lookup request for key "{key}" to node {successor_id}')
        return self.call_rpc(successor_addr, 'lookup', key)

    def stop(self):
        """
        Stop the Chord node gracefully.
        """
        self.stop_event.set()
        # Close the server socket if it's open
        try:
            self.server.close()
        except Exception:
            pass

    def print_finger_table(self):
        """
        Print the finger table of the node.
        """
        print(f'\n[Node {self.id}] [Finger Table Details] Finger Table:')
        for i in range(1, M + 1):
            entry = self.finger[i]
            print(f'  Entry {i}: Start={entry.start}, Interval={entry.interval.intervals}, Node={entry.node[0]}')

    def node_Addition(self):
        """
        Extra Credit: Dynamically add new nodes after data population, triggering finger table adjustments.
        """
        # Create a new node with a unique ID and port
        new_node_id = random.randint(0, NODES - 1)
        new_node_port = random.randint(10000, 20000)  # Assign a random port for the new node
        new_node = ChordNode(new_node_port)

        # Notify all nodes to refresh their finger tables
        self.update_others()
        
        # Simulate migration of data to the new node
        for key in list(self.data.keys()):
            key_id = self.hash(key)
            # Check if the new node should take responsibility for this key
            if key_id in ModRange(self.id + 1, new_node_id + 1, NODES):
                # Move the key to the new node
                value = self.data.pop(key)
                new_node.store(key, value)

        # Display the updated finger table after adding the new node
        self.print_finger_table()

    def integrity_nodes(self):
        """
        Ensure the new node is correctly integrated into the network by stabilizing finger table entries.
        """
        # Validate that finger table entries correctly point to the appropriate nodes
        for i in range(1, M + 1):
            if not self.finger[i].node or self.finger[i].node[0] == self.id:
                continue  # Skip if the finger entry is self-referencing or empty

            node_id, node_addr = self.finger[i].node
            if node_id != self.id and node_id in ModRange(self.id + 1, node_id + 1, NODES):
                print(f"\n[Node {self.id}] [Integrity] Finger entry {i} correctly points to node {node_id}.")
            else:
                # Update the finger table entry if necessary
                new_node = self.closest_preceding_finger(self.finger[i].start)
                self.finger[i].node = new_node
                print(f"\n[Node {self.id}] [Integrity] Finger entry {i} updated to point to node {new_node[0]}.")

        """
        Helper function for Extra Credit: Add nodes after data population.
        Perform stabilization to ensure that the new node is correctly linked in the network.
        """
        # Check if the newly added node affects the finger table entries
        for i in range(1, M + 1):
            if self.finger[i].node is None or self.finger[i].node[0] == self.id:
                continue  # Skip if no finger or finger points to self

            node_id, node_addr = self.finger[i].node
            if node_id != self.id and node_id in ModRange(self.id + 1, node_id + 1, NODES):
                print(f"\n[Node {self.id}] [Integrity Details] Finger table entry {i} correctly points to node {node_id}.")
                continue
            else:
                # Update finger table entry
                new_node = self.closest_preceding_finger(self.finger[i].start)
                self.finger[i].node = new_node
                print(f"\n[Node {self.id}] [Integrity Details] Updated finger table entry {i} to point to new node {new_node[0]}.")
 
 
       
if __name__ == '__main__':
    """
    Main Function for the Chord DHT node.
    
    Usage:
        python chord_node.py <previous_port_number>
    
    Arguments:
        previous_port_number (int): The port of a known node in the network. Use 0 to start a new network.
    """
    if len(sys.argv) != 2:
        print('Usage: python chord_node.py <previous_port_number>')
        print('Use 0 as known_port to start a new network.')
        sys.exit(1)
    
    previous_port_number = int(sys.argv[1])
    node = ChordNode(previous_port_number)
    
    try:
        # Keep the node active
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Stop the node on keyboard interrupt
        node.stop()
        print(f'\n[Node {node.id}] Node shut down.')
        sys.exit(0)