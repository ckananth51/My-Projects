import socket
import threading

def receive_messages(sock):
    while True:
        try:
            data = sock.recv(4096).decode()
            if data:
                print(data)
        except Exception as e:
            print("[Error] Lost connection to server.")
            break

def connect_to_server():
    while True:
        try:
            host = input("Enter server IP to connect: ").strip()  # Enter the server's IP address
            port = int(input("Enter server port to connect: "))
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            print(f"Connected to server at {host}:{port}")
            return sock
        except Exception as e:
            print(f"[Error] Could not connect to server: {e}")
            print("Please try a different IP/port.")

def main():
    while True:
        # Attempt to connect (or reconnect) to the server
        s = connect_to_server()
        threading.Thread(target=receive_messages, args=(s,), daemon=True).start()
        
        while True:
            msg = input()
            if msg.lower() == 'exit':
                s.close()
                return
            try:
                s.sendall(bytes(f'{{"message": "{msg}"}}', 'utf-8'))
            except Exception as e:
                print("[Error] Connection lost. Leader may have crashed.")
                s.close()
                break

if __name__ == "__main__":
    main()
