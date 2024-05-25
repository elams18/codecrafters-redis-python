import socket

def client():
    client = socket.create_connection(("localhost", 6379),)
    client.send("PING".encode())
    data1 = client.recv(7)
    print(data1)
    client.send("PING".encode())
    data2 = client.recv(7)
    print(data2)

if __name__ == "__main__":
    client()