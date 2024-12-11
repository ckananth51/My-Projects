import socket

class TicTacToeClient:
    def __init__(self, host='172.20.10.4', port=12783):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.host, self.port))

    def start(self):
        while True:
            data = self.client_socket.recv(1024).decode()
            if "Enter a coordinate" in data:
                print(data)
                move = input("Enter your move: ")
                self.client_socket.sendall(move.encode())
            elif "wins" in data or "draw" in data:
                print(data)
                break
            else:
                print(data)

if __name__ == "__main__":
    client = TicTacToeClient()
    client.start()
