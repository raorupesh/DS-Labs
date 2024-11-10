"""
CPSC 5520, Seattle University
Assignment Name: Pub/Sub Assignment
Author: Rupeshwar Rao
"""


import socket
import struct
import math
import time
from datetime import datetime, timedelta

from fxp_bytes_subscriber import parse_message
from bellman_ford import BellmanFord

MICROS_PER_SECOND = 1_000_000


def get_local_ip():
    """
    Retrieve the local IP address of the machine.

    Returns:
        str: Local IP address as a string.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(('10.255.255.255', 1))
            local_ip = s.getsockname()[0]
        except Exception:
            local_ip = '127.0.0.1'
    return local_ip


def subscribe_to_forex(sock, provider_address, local_ip, port):
    """
    Send a subscription request to the forex provider.

    Args:
        sock: The UDP socket to use for communication.
        provider_address: The address of the forex provider.
        local_ip: The local IP address to include in the request.
        port: The port number to include in the request.
    """
    ip_bytes = socket.inet_aton(local_ip)
    port_bytes = struct.pack('!H', port)
    subscription_request = ip_bytes + port_bytes
    sock.sendto(subscription_request, provider_address)
    print(f'Sent subscription request to {provider_address}')


def handle_message(data, latest_timestamps, quotes):
    """
    Process the received message and update the quotes dictionary.

    Args:
        data: The received data from the forex provider.
        latest_timestamps: A dictionary to track the latest timestamps for each currency pair.
        quotes: A dictionary to store the latest quotes for currency pairs.
    """
    quotes_list = parse_message(data)
    for quote in quotes_list:
        currency_from = quote['currency1']
        currency_to = quote['currency2']
        exchange_rate = quote['rate']
        timestamp = quote['timestamp']
        market_pair = (currency_from, currency_to)
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')

        if market_pair in latest_timestamps:
            if timestamp <= latest_timestamps[market_pair]:
                print(f'{timestamp_str} {currency_from} {currency_to} {exchange_rate}')
                print('Ignoring out-of-sequence message')
                continue

        latest_timestamps[market_pair] = timestamp
        expiration_time = timestamp + timedelta(seconds=1.5)
        quotes[market_pair] = {'rate': exchange_rate, 'timestamp': timestamp, 'expiration': expiration_time}
        print(f'{timestamp_str} {currency_from} {currency_to} {exchange_rate}')


def remove_expired_quotes(quotes):
    """
    Remove expired quotes from the quotes dictionary.

    Args:
        quotes: A dictionary containing the latest quotes for currency pairs.
    """
    current_time = datetime.utcnow()
    expired_markets = []

    for market_pair in quotes:
        expiration_time = quotes[market_pair]['expiration']
        if current_time > expiration_time:
            expired_markets.append(market_pair)

    for market_pair in expired_markets:
        del quotes[market_pair]
        print(f'Removing stale quote for {market_pair}')


def create_graph(quotes):
    """
    Build a graph from the current quotes for use in the Bellman-Ford algorithm.

    Args:
        quotes: A dictionary containing the latest quotes for currency pairs.

    Returns:
        tuple: A tuple containing the graph object and a dictionary of edge rates.
    """
    graph = BellmanFord()
    edge_rates = {}

    for market_pair in quotes:
        currency_from, currency_to = market_pair
        exchange_rate = quotes[market_pair]['rate']
        weight_forward = -math.log(exchange_rate)
        graph.add_edge(currency_from, currency_to, weight_forward)
        edge_rates[(currency_from, currency_to)] = exchange_rate

        weight_reverse = math.log(exchange_rate)
        graph.add_edge(currency_to, currency_from, weight_reverse)
        edge_rates[(currency_to, currency_from)] = 1 / exchange_rate

    return graph, edge_rates


def find_negative_cycle(graph):
    """
    Run the Bellman-Ford algorithm to detect negative cycles in the graph.

    Args:
        graph: The graph object containing currency pairs and their weights.

    Returns:
        list: A list of currencies forming a negative cycle, or None if no cycle exists.
    """
    vertices = graph.vertices
    if not vertices:
        return None

    start_vertex = next(iter(vertices))
    distances, predecessors, negative_cycle_edge = graph.shortest_paths(start_vertex)

    if negative_cycle_edge:
        cycle = []
        u, v = negative_cycle_edge
        cycle.append(v)
        current = u
        while current not in cycle:
            cycle.append(current)
            current = predecessors[current]
        cycle.append(current)
        cycle.reverse()
        return cycle
    else:
        return None


def report_arbitrage_opportunity(cycle, edge_rates):
    """
    Report an arbitrage opportunity based on the detected cycle.

    Args:
        cycle: A list of currencies forming the arbitrage cycle.
        edge_rates: A dictionary of exchange rates for currency pairs.
    """
    log = []
    log.append("ARBITRAGE:")
    initial_amount = 100.0  # Starting with USD 100
    current_currency = "USD"
    log.append(f'\tStart with {current_currency} {initial_amount}')

    for i in range(len(cycle) - 1):
        next_currency = cycle[i + 1]
        rate = edge_rates.get((current_currency, next_currency))

        if rate is None:
            log.append(f'Rate from {current_currency} to {next_currency} not found.')
            return log

        initial_amount *= rate
        log.append(f'\tExchange {current_currency} for {next_currency} at {rate} --> {next_currency} {initial_amount}')
        current_currency = next_currency

    # Convert back to USD if the last currency is not USD
    if current_currency != "USD":
        rate = edge_rates.get((current_currency, "USD"))
        if rate is None:
            log.append(f'No exchange rate available to convert {current_currency} back to USD.')
            return log
        initial_amount *= rate
        log.append(f'\tExchange {current_currency} for USD at {rate} --> USD {initial_amount}')
    else:
        log.append(f'Final amount in USD: {initial_amount}')

    if initial_amount > 100:
        print("\n".join(log))


def main():
    # Create a UDP socket
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(('', 0))
        local_ip = get_local_ip()
        port = sock.getsockname()[1]

        print(f'Subscribing with IP {local_ip} and port {port}')

        # Forex provider address (adjust as needed)
        forex_provider_address = ('localhost', 50403)
        subscribe_to_forex(sock, forex_provider_address, local_ip, port)

        start_time = time.time()
        latest_timestamps = {}
        quotes = {}

        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time > 10 * 60:
                print('Subscription period over. Exiting.')
                break

            sock.settimeout(10)
            try:
                data, address = sock.recvfrom(4096)
                handle_message(data, latest_timestamps, quotes)
                remove_expired_quotes(quotes)
                graph, edge_rates = create_graph(quotes)
                cycle = find_negative_cycle(graph)
                if cycle:
                    report_arbitrage_opportunity(cycle, edge_rates)
            except socket.timeout:
                print('No messages received for 10 seconds. Exiting.')
                break


if __name__ == '__main__':
    main()
