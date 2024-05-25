import socket
import threading
import re
from datetime import datetime, timedelta
import time
import argparse

def handle_client(client, redis_data: dict):
    def split_segments(s):
        segments = s.split('$')
        if segments[0].startswith('*'):
            segments[0] = segments[0][1:]
        segments = [seg for seg in segments if seg]
        segments = [seg if not seg.endswith('$') else seg for seg in segments]
        processed_segments = []
        for seg in segments:
            processed_segment = re.sub(r'^\d+\r\n', '', seg)
            processed_segment = processed_segment.rstrip('\r\n')
            processed_segments.append(processed_segment)
        return processed_segments
    
    def encode_bulk_string(s: str) -> bytes:
        return ("$"+ str(len(s)) + "\r\n" + s + "\r\n").encode()
    
    def encode_error_message(err: str) -> bytes:
        return ("-ERR " + err+ "\r\n").encode()
    
    while client:
        req = client.recv(4096)
        data: str = req.decode() 
        cmds_list = split_segments(data)
        cmds = iter(cmds_list)
        if not cmds_list:
            break
        while True:
            if not cmds_list:
                break
            try:
                cmd = next(cmds)
            except StopIteration:
                break
            if cmd == '':
                continue
            if cmd.lower() == 'set':
                key = None
                value = None
                try:
                    key = next(cmds)
                    value = next(cmds)
                    expiry_cmd = next(cmds)
                    expiry = None
                    if expiry_cmd.lower() == 'px': 
                        ms = int(next(cmds))
                        expiry = datetime.now() + timedelta(milliseconds=ms)
                    redis_data[key] = {"value": value, "expiry": expiry}
                    client.send(b"+OK\r\n")  
                except StopIteration:
                    # if there is no expiration added, just send the value
                    if key and value:
                        redis_data[key] = {"value": value}
                        client.send(b"+OK\r\n")
                    else:
                        client.send(b"$-1\r\n")  
                    break
            if cmd.lower() == 'get':
                try:
                    key = next(cmds)
                    get_data = redis_data.get(key)
                    if get_data:
                        value = get_data['value']
                        expiry = get_data.get('expiry')
                        if expiry and expiry < datetime.now():
                            del redis_data[key]  # Remove expired key
                            client.send(b"$-1\r\n")
                        else:
                            client.send(encode_bulk_string(value))
                    else:
                        client.send(b"$-1\r\n")
                except StopIteration:
                    client.send(b"$-1\r\n")
                    break
            if cmd.lower() == 'ping':
                client.send(b"+PONG\r\n")
                break
            if cmd.lower() == 'echo':
                try:
                    content = "+" + next(cmds) + "\r\n"
                    client.send(content.encode())
                    break
                except StopIteration:
                    err = encode_error_message("echo msg not given")
                    client.send(err)
                    break

def expiration_cleanup(redis_data: dict):
    while True:
        # Iterate over keys and remove expired keys
        for key in list(redis_data.keys()):
            if redis_data[key].get('expiry') and redis_data[key]['expiry'] < datetime.now():
                del redis_data[key]
        # Sleep for some time before checking again (e.g., every minute)
        time.sleep(60)

def main(port=6379):
    print(f"Logs from your program will appear here in port {port}!")
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    redis_data = {}

    # Start expiration cleanup thread
    cleanup_thread = threading.Thread(target=expiration_cleanup, args=(redis_data,))
    cleanup_thread.daemon = True
    cleanup_thread.start()

    while True:
        client, address = server_socket.accept() # wait for client
        thread = threading.Thread(target=handle_client, args=(client, redis_data))
        thread.start()

if __name__ == "__main__":
    argsParser = argparse.ArgumentParser("A Redis server written in Python")
    argsParser.add_argument("--port", type=int, dest="port", default=6379)
    args = argsParser.parse_args()
    port = args.port

    main(port)
