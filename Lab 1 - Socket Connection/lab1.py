"""
Author: Rupeshwar Maheshwar Rao
Assignment: Lab1 Simple Client
Class: CPSC 5520 01, Seattle University
Submission Date: 03rd October 2024
"""
import sys
import pickle
import socket


def gcd_connection_task(hostname, port):
    """
    Explanation for the below gcd_connection_task Code:
    1. Use the socket library to use AF_INET type socket and SOCK_STREAM protocol
    2. Connect to the hostname and port provide as parameters provided.
    3. Send the request to the host and receive the response.
    4. Used pickle to load the data return by host and return it.
    """
    print("Initiated Process of connecting to GCD: \n")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_details:
            socket_details.connect((hostname, port))
            print(f"BEGIN ({hostname}, {port} )")
            socket_details.sendall(pickle.dumps('BEGIN'))
            data = socket_details.recv(4096)
            group_members = pickle.loads(data)
            print("Completed Process of Retreiving Data from GCD. \n")
            return group_members
        
    except socket.error as exception:
        print(f"Attempt to connect to GCD failed due to the exception: {exception}")
        sys.exit(1)


def groupmember_connection_task(individual_member):
    """
    Explanation for the below groupmember_connection_task Code:
    1. Similar to above function using socket library.
    2. Connecting with each host on the required port
    3. Once the connection has been establsihed send the HELLO request to it.
    4. Collect the response returned but it, print it and return it.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as member_socket:
            member_socket.settimeout(1.5)
            member_socket.connect((individual_member['host'], individual_member['port']))
            print(f"HELLO to {individual_member}")
            member_socket.sendall(pickle.dumps('HELLO'))
            response_data = member_socket.recv(4096)
            response = pickle.loads(response_data)
            print(response)
            
    except socket.timeout:
        print(f"Timeout occurred for group member {individual_member['host']}:{individual_member['port']}")
        
    except socket.error as exception:
        print(f"HELLO to {individual_member}")
        print(f"failed to connect: {{}} {exception}")
        


def main():
    """
    Explanation for the main function:
    1. If 3 arguments(filename, hostname and port) are not provided while running the code show the exception.
    2. Hostname and port entered via terminal is stored and send to gcd_connection_task function.
    3. Check if null members are returned or not, if not then run a for loop and send each member to the 
        groupmember_connection_task function.
    """
    if len(sys.argv) != 3:
        print("Please use the following pattern of command: python lab1.py <hostname> <port>")
        sys.exit(1)
        
    gcd_hostname = sys.argv[1]
    gcd_port = int(sys.argv[2])
    all_group_members = gcd_connection_task(gcd_hostname, gcd_port)
    

    if all_group_members:
        print("Initiated sending message to each host: \n")
        for each_member in all_group_members:
            groupmember_connection_task(each_member) 
        print("Received all messages sent back from GCD. \n")
    else:
        print("Group members were not found.")


if __name__ == '__main__':
    main()