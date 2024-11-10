from socketserver import ThreadingTCPServer, BaseRequestHandler
import threading
import sys
import socket
import pickle
import random
import time as timestamp
from datetime import *


# Declarations
group_members: dict = {}
higher_members: dict = {}
leader = None
election_in_progress = False

def gcd_connection():
    
    """Communication initated with the GCD to register and get initial list of memebers"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listner:
        listner.connect((gcd_host, gcd_port))
        print(f"BEGIN ({gcd_host, gcd_port}) ({unique_id}) ({listen_host, listen_port})")
        print(f"Sending BEGIN ({unique_id}) ({listen_host, listen_port})")
        listner.sendall(pickle.dumps(('BEGIN', (unique_id, (listen_host, listen_port)))))
        print(f"Receiving: ({unique_id}: {listen_host, listen_port})")
        data = pickle.loads(listner.recv(1024))
        print(f"Members: ({unique_id}: {data})")
        return data

def start_election():
    
    """Initiation of an Election
    1. Sets the Election Progress value to true.
    2. If any higher node is found declare victory else
    3. Sends Election message to all nodes
    """
    global election_in_progress, leader, higher_members

    election_in_progress = True
    higher_members = {}  # Reset higher_members
    print(f"Starting election with ID: {unique_id}")
    higher_members = {k: v for k, v in group_members.items() if k > unique_id}

    # If no higher members are present then declare self as the winner
    if not higher_members:
        victory_declration()
        
    else:
        # Send ELECTION messages to all higher members
        responses = []
        for addr in higher_members.values():
            response = send_message(addr, ('ELECTION', group_members))
            if response == 'OK':
                responses.append(response)

        # If no responses are given declare self as winner
        if not responses:
            victory_declration()

def victory_declration():
    """Declaring self as the new leader, stopping the election process and setting the election in progress variable to false"""
    global leader, election_in_progress
    leader = unique_id
    election_in_progress = False
    print(f"Victory by {leader}, no other bullies bigger than me.")

    # Send COORDINATOR message to all members
    for ids, addr in group_members.items():
        send_message(addr, ('COORDINATOR', leader))

def send_message(address, message):
    """Send a pickled message to the given address."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(address)
            s.sendall(pickle.dumps(message))
            print(f"Sending {message} to {address} ({threading.get_ident()})")
            return pickle.loads(s.recv(1024))
    except Exception as e:
        return f"Error in sending message as {e}."

class PeerHandler(BaseRequestHandler):
    
    """Handles incoming messages from other members.
    Depending on the message type the respective action(function calls) is completed
    """

    def handle(self):
        global election_in_progress, leader, group_members

        try:
            print(f"\nSTARTING PROCESS for pid {unique_id} on {self.client_address} ")
            print(f"BEGIN {self.server.server_address}, {unique_id}")
            message_name, message_data = pickle.loads(self.request.recv(1024))
            print(f"Receiving {message_data} from {threading.get_ident()}")

            if message_name == 'BEGIN':
                print(f"Members: {group_members}. Starting an election at startup.")
                start_election()

            if message_name == 'ELECTION':
                group_members.update(message_data)
                self.request.sendall(pickle.dumps('OK'))

                if not election_in_progress:
                    start_election()

            elif message_name == 'COORDINATOR':
                leader = message_data
                election_in_progress = False
                print(f"New leader elected: {leader}")

            elif message_name == 'PROBE':
                self.request.sendall(pickle.dumps('OK'))

        except Exception as e:
            print(f"Error handling peer message: {e}")

if __name__ == '__main__':

    if len(sys.argv) != 5:
        print("Usage: python lab2.py <hostname> <port> <days_left_for_birthday> <su_id>")
        sys.exit(1)

    gcd_host = sys.argv[1]
    gcd_port = int(sys.argv[2])
    days_left_for_birthday = int(sys.argv[3])
    su_id = int(sys.argv[4])

    unique_id = (days_left_for_birthday, su_id)
    listen_host = 'localhost'
    listen_port = random.randint(10000, 60000)

    upcoming_birthday = datetime.now() + timedelta(days=days_left_for_birthday)
    print(f"Next Birthday on: {upcoming_birthday}")
    print(f"SeattleU ID: {su_id}")

    # Start the listening server
    server = ThreadingTCPServer((listen_host, listen_port), PeerHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Server Loop Running in Thread: {threading.current_thread().name}")

    # Register with GCD and get initial list of members
    group_members = gcd_connection()
    start_election()

    # Keep the main thread alive
    try:
        while True:
            timestamp.sleep(1)
    except KeyboardInterrupt:
        print("Closing Terminal")
        server.shutdown()
