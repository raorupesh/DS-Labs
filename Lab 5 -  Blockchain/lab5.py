"""
CPSC 5520, Seattle University
Assignment Name: BlockChain
Author: Rupeshwar Rao
"""

import socket
import time
import hashlib
import struct

# Defined the port number for the Bitcoin network
PORT = 8333            

# Defined the host IP address for the Bitcoin network
HOST = '139.84.239.58'

# Defined the protocol version
VERSION = 70015

# Defined the size of the message header
HEADER_SIZE = 24

# Defined the buffer size for receiving messages
BUFFER_SIZE = 2048

# Defined the block number to search for (modulus 10000)
BLOCK_NUMBER = 172489 % 10000

# Defined the number of blocks to process per iteration
PER_ITERATION_COUNT = 500

def compactsize_t(n):
    """Convert an integer to Bitcoin's compact size format."""
    if n < 252:
        return uint8_t(n)
    if n < 0xffff:
        return uint8_t(0xfd) + uint16_t(n)
    if n < 0xffffffff:
        return uint8_t(0xfe) + uint32_t(n)
    return uint8_t(0xff) + uint64_t(n)

def unmarshal_compactsize(b):
    """Unmarshal a compact size integer from bytes."""
    key = b[0]
    if key == 0xff:
        return b[0:9], unmarshal_uint(b[1:9])
    if key == 0xfe:
        return b[0:5], unmarshal_uint(b[1:5])
    if key == 0xfd:
        return b[0:3], unmarshal_uint(b[1:3])
    return b[0:1], unmarshal_uint(b[0:1])

def bool_t(flag):
    """Convert a boolean to a uint8_t."""
    return uint8_t(1 if flag else 0)

def ipv6_from_ipv4(ipv4_str):
    """Convert an IPv4 address to an IPv6 address."""
    pchIPv4 = bytearray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff])
    return pchIPv4 + bytearray((int(x) for x in ipv4_str.split('.')))

def ipv6_to_ipv4(ipv6):
    """Convert an IPv6 address to an IPv4 address."""
    return '.'.join([str(b) for b in ipv6[12:]])

def uint8_t(n):
    """Convert an integer to a uint8_t."""
    return int(n).to_bytes(1, byteorder='little', signed=False)

def uint16_t(n):
    """Convert an integer to a uint16_t."""
    return int(n).to_bytes(2, byteorder='little', signed=False)

def int32_t(n):
    """Convert an integer to an int32_t."""
    return int(n).to_bytes(4, byteorder='little', signed=True)

def uint32_t(n):
    """Convert an integer to a uint32_t."""
    return int(n).to_bytes(4, byteorder='little', signed=False)

def int64_t(n):
    """Convert an integer to an int64_t."""
    return int(n).to_bytes(8, byteorder='little', signed=True)

def uint64_t(n):
    """Convert an integer to a uint64_t."""
    return int(n).to_bytes(8, byteorder='little', signed=False)

def unmarshal_int(b):
    """Unmarshal an integer from bytes."""
    return int.from_bytes(b, byteorder='little', signed=True)

def unmarshal_uint(b):
    """Unmarshal an unsigned integer from bytes."""
    return int.from_bytes(b, byteorder='little', signed=False)

def calculate_checksum(payload: bytes):
    """Calculate the checksum of a payload."""
    if len(payload) == 0:
        return bytes.fromhex('5df6e0e2')
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    return checksum

def hex_littletobig(string):
    """Convert a little-endian hex string to big-endian."""
    result = bytearray.fromhex(string)
    result.reverse()
    return ''.join(format(x, '02x') for x in result)

def print_block_msg(b):
    """
    Display the transactions in the desired block.
    This function takes a block of data and prints its details, including version, previous hash, Merkle root, 
    timestamp, number of bits, and nonce. It also processes the remaining part of the block to extract 
    transaction information.
        
        Extra Credit: Display the transactions in desired block (EC)
    """
    print('\n Displaying Desired BLOCK Details')
    print('-' * 100)
    prefix = '  '

    version = b[:4]
    prev_header_hash = hex_littletobig(b[4:36].hex())
    merkle_root_hash = hex_littletobig(b[36:68].hex())
    unix_time = b[68:72]
    time_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT",
                             time.gmtime(unmarshal_int(unix_time)))
    nbits = hex_littletobig(b[72:76].hex())
    nonce = hex_littletobig(b[76:80].hex())

    print('{}{:80} version {}'.format(prefix, version.hex(), unmarshal_int(version)))
    print('{}{:80} Previous Hash'.format(prefix, prev_header_hash))
    print('{}{:80} Merkle Root '.format(prefix, merkle_root_hash))
    print('{}{:80} epoch {}'.format(prefix, unix_time.hex(), time_str))
    print('{}{:80} number of bits'.format(prefix, nbits))
    print('{}{:80} nonce'.format(prefix, nonce))

    split = b[80:].split(bytes.fromhex('01000000'))
    key, count = unmarshal_compactsize(split[0])

def print_inv_msg(b, iteration):
    """Print the details of an inventory message."""
    if iteration == 0 or (BLOCK_NUMBER / PER_ITERATION_COUNT) - iteration < 2:
        print('\n''INV MESSAGE')
        print('-' * 100)
        print('BLOCKNO', b[:3].hex(),
              'HASH KEY                                                        ',   
              'MSG_TYPE',
              'INVENTORY')
    count = 1
    iteration_start = iteration * 500
    num_bytes = 36
    remainder = ''
    for i in range(3, len(b), num_bytes):
        try:
            block = b[i:i + num_bytes].hex()
            starter = block[:8]
            remainder = hex_littletobig(block[8:])
            if iteration_start + count == BLOCK_NUMBER:
                print('#'+str(iteration_start + count),starter, remainder, 'MSG_TXT',
                      'inventory')
                print("\n" + "BLOCK ID % 10000 has been found: #" + str(iteration_start + count) + " at " +remainder )
                return remainder, True
            if iteration == 0 or (BLOCK_NUMBER / PER_ITERATION_COUNT) - iteration < 2:
                print("#"+str(iteration_start + count),starter, remainder, 'MSG_TXT',
                      'inventory')
            count += 1
        except Exception:
            continue
    return remainder, False

def print_message(msg, text=None, iteration=0):
    """Report the contents of the given bitcoin message."""
    print('\n{}MESSAGE'.format('' if text is None else (text + ' ')))
    print('({}) {}'.format(len(msg),
                           msg[:60].hex() + ('' if len(msg) < 60 else '...')))
    payload = msg[HEADER_SIZE:]
    command = print_header(msg[:HEADER_SIZE], calculate_checksum(payload))

    highest = ''
    found = False

    if command == 'version':
        print_version_msg(payload)
    elif command == 'inv':
        highest, found = print_inv_msg(payload, iteration)
    elif command == 'block':
        print_block_msg(payload)
    return command, highest, found

def print_version_msg(b):
    """Report the contents of the given bitcoin version message (sans the header)."""
    version, my_services, epoch_time, your_services = b[:4], b[4:12], b[12:20], b[20:28]
    rec_host, rec_port, my_services2, my_host, my_port = b[28:44], b[44:46], b[46:54], b[54:70], b[70:72]
    nonce = b[72:80]
    user_agent_size, uasz = unmarshal_compactsize(b[80:])
    i = 80 + len(user_agent_size)
    user_agent = b[i:i + uasz]
    i += uasz
    start_height, relay = b[i:i + 4], b[i + 4:i + 5]
    extra = b[i + 5:]

    prefix = '  '
    print(prefix + 'VERSION')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} version {}'.format(prefix, version.hex(), unmarshal_int(version)))
    print('{}{:32} my services'.format(prefix, my_services.hex()))
    time_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(unmarshal_int(epoch_time)))
    print('{}{:32} epoch time {}'.format(prefix, epoch_time.hex(), time_str))
    print('{}{:32} your services'.format(prefix, your_services.hex()))
    print('{}{:32} your host {}'.format(prefix, rec_host.hex(), ipv6_to_ipv4(rec_host)))
    print('{}{:32} your port {}'.format(prefix, rec_port.hex(), unmarshal_uint(rec_port)))
    print('{}{:32} my services (again)'.format(prefix, my_services2.hex()))
    print('{}{:32} my host {}'.format(prefix, my_host.hex(), ipv6_to_ipv4(my_host)))
    print('{}{:32} my port {}'.format(prefix, my_port.hex(), unmarshal_uint(my_port)))
    print('{}{:32} nonce'.format(prefix, nonce.hex()))
    print('{}{:32} user agent size {}'.format(prefix, user_agent_size.hex(), uasz))
    print('{}{:32} user agent \'{}\''.format(prefix, user_agent.hex(), str(user_agent, encoding='utf-8')))
    print('{}{:32} start BLOCK_NUMBER {}'.format(prefix, start_height.hex(), unmarshal_uint(start_height)))
    print('{}{:32} relay {}'.format(prefix, relay.hex(), bytes(relay) != b'\0'))
    if len(extra) > 0:
        print('{}{:32} EXTRA!!'.format(prefix, extra.hex()))

def print_header(header, expected_cksum=None):
    """Report the contents of the given bitcoin message header."""
    magic, command_hex, payload_size, cksum = header[:4], header[4:16], header[16:20], header[20:]
    command = str(bytearray([b for b in command_hex if b != 0]), encoding='utf-8')
    payload_size = unmarshal_uint(payload_size)

    if expected_cksum is None:
        verified = ''
    elif expected_cksum == cksum:
        verified = '(verified)'
    else:
        verified = '(WRONG!! ' + expected_cksum.hex() + ')'
    prefix = '  '
    print(prefix + 'HEADER')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} magic'.format(prefix, magic.hex()))
    print('{}{:32} command: {}'.format(prefix, command_hex.hex(), command))
    print('{}{:32} payload size: {}'.format(prefix, uint32_t(payload_size).hex(), payload_size))
    print('{}{:32} checksum {}'.format(prefix, cksum.hex(), verified))
    return command

class BlockchainLab:
    """A class to represent a Bitcoin client."""

    def __init__(self):
        self.socket, self.local_address = self.initialize_socket()

    def initialize_socket(self):
        """Initialize a TCP socket."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        return sock, sock.getsockname()

    def handle_message(self, message, command='', iteration=0):
        """Handle a Bitcoin message."""
        print_message(message, 'SENDING')
        self.socket.send(message)
        response = self.socket.recv(BUFFER_SIZE)
        messages = self.split_messages(response)
        command_received, highest_block, block_found = '', '', False

        for msg in messages:
            payload = msg[HEADER_SIZE:]
            checksum = calculate_checksum(payload)
            header = msg[:HEADER_SIZE]
            header_checksum = header[20:]

            while checksum != header_checksum:
                additional_data = self.socket.recv(BUFFER_SIZE)
                split_data = additional_data.hex().partition('f9beb4d9')

                payload += bytes.fromhex(split_data[0])
                messages.extend(self.split_messages(bytes.fromhex(split_data[2])))
                checksum = calculate_checksum(payload)
            command_received, highest_block, block_found = print_message(header + payload, 'RECEIVED', iteration)

        if command == "getblocks":
            if command_received != 'inv':
                return self.handle_message(msg, command)

        return highest_block, block_found

    def split_messages(self, message):
        """Split the received message into individual Bitcoin messages."""
        hex_message = message.hex()
        message_parts = hex_message.split('f9beb4d9')
        parsed_messages = []
        for part in range(1, len(message_parts)):
            encoded_message = bytes.fromhex('f9beb4d9' + message_parts[part])
            parsed_messages.append(encoded_message)
        return parsed_messages

    def locate_block(self, highest_block, block_found):
        """Locate a specific block in the blockchain."""
        iteration = 1
        while not block_found:
            getblocks_payload = self.create_getblocks_payload(False, highest_block)
            getblocks_header = self.create_header(getblocks_payload, 'getblocks')
            getblocks_message = getblocks_header + getblocks_payload
            highest_block, block_found = self.handle_message(getblocks_message, 'getblocks', iteration)
            iteration += 1
        return highest_block

    def create_header(self, payload, command):
        """Create a Bitcoin message header."""
        magic = bytearray.fromhex('F9BEB4D9')
        command = struct.pack("12s", command.encode())
        length = uint32_t(len(payload))
        checksum = calculate_checksum(payload)
        return magic + command + length + checksum

    def create_message(self, payload, command):
        """Create a Bitcoin message."""
        magic = bytes.fromhex("F9BEB4D9")
        command = command + (5 * "\00")
        length = uint32_t(len(payload))
        checksum = calculate_checksum(payload)
        return magic + bytes(command.encode()) + length + checksum + payload

    def create_getblocks_payload(self, genesis, highest_block=''):
        """Create the payload for a getblocks message."""
        version = int32_t(VERSION)
        count = compactsize_t(1)

        if genesis:
            block_hash = struct.pack("32s", b'\x00')
        else:
            block_hash = bytearray.fromhex(hex_littletobig(highest_block))

        hash_stop = struct.pack("32s", b'\x00')
        return version + count + block_hash + hash_stop

    def create_getdata_payload(self, block_hash):
        """Create the payload for a getdata message."""
        count = compactsize_t(1)
        verbosity = uint32_t(2)
        block = bytearray.fromhex(hex_littletobig(block_hash))
        return count + verbosity + block

    def create_version_message(self, host, command):
        """Create a version message."""
        version = int32_t(VERSION)
        services = uint64_t(0)
        timestamp = int64_t(int(time.time()))

        addr_recv_services = uint64_t(1)
        addr_recv_ip = ipv6_from_ipv4(host)
        addr_recv_port = uint16_t(PORT)

        addr_trans_services = uint64_t(0)
        addr_trans_ip = ipv6_from_ipv4(self.local_address[0])
        addr_trans_port = uint16_t(self.local_address[1])

        nonce = uint64_t(0)
        user_agent_bytes = compactsize_t(0)
        starting_height = int32_t(0)
        relay = bool_t(False)

        payload = (version + services + timestamp + addr_recv_services + addr_recv_ip + addr_recv_port +
                   addr_trans_services + addr_trans_ip + addr_trans_port + nonce + user_agent_bytes +
                   starting_height + relay)
        return self.create_message(payload, command)

    def create_verack_message(self):
        """Create a verack message."""
        return bytearray.fromhex("f9beb4d976657261636b000000000000000000005df6e0e2")
    
    def create_invalid_output_transaction(self):
        """
        Simulate a transaction with an invalid output that will be rejected by the system.
        
        Extra Credit: Report showing how a change in an output account will not be accepted by the system. (EC)
        
        """
        version = struct.pack("<L", 1)
        lock_time = struct.pack("<L", 0)
        
        # Let's simulate an invalid output (incorrect address or format)
        invalid_output_script = bytearray([0x6a])  # OP_RETURN is often used for invalid scripts
        invalid_output_value = struct.pack("<Q", 0)  # 0 value (this would never be accepted)

        # Transaction inputs (for simplicity, no real input data)
        tx_inputs = bytearray([0x01, 0x00, 0x00, 0x00])  # Invalid input data
        
        # Creating the raw transaction (this would normally be more complex)
        raw_tx = version + tx_inputs + invalid_output_value + invalid_output_script + lock_time
        return raw_tx

if __name__ == '__main__':
    # Create an instance of BlockchainLab
    bitcoin_client = BlockchainLab()

    # Connect to the Bitcoin network
    bitcoin_client.socket.connect((HOST, PORT))

    # Send version message to the Bitcoin network
    version_message = bitcoin_client.create_version_message(HOST, 'version')
    bitcoin_client.handle_message(version_message)

    # Send verack message to acknowledge the version message
    verack_message = bitcoin_client.create_verack_message()
    bitcoin_client.handle_message(verack_message)

    # Send getblocks message to request blocks starting from the genesis block
    getblocks_payload = bitcoin_client.create_getblocks_payload(True)
    getblocks_header = bitcoin_client.create_header(getblocks_payload, 'getblocks')
    getblocks_message = getblocks_header + getblocks_payload

    # Process the getblocks message
    highest_block, block_found = bitcoin_client.handle_message(getblocks_message, 'getblocks')
    highest_block = bitcoin_client.locate_block(highest_block, block_found)

    # Send getdata message to request the data for the found block
    getdata_payload = bitcoin_client.create_getdata_payload(highest_block)
    getdata_header = bitcoin_client.create_header(getdata_payload, 'getdata')
    getdata_message = getdata_header + getdata_payload
    bitcoin_client.handle_message(getdata_message, 'getdata')
