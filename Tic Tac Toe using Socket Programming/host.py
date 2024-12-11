import socket
import threading
import time

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

class TicTacToeServer:
    def __init__(self, host=None, port=12783):
        self.host = host if host else get_local_ip()  # Automatically fetches the host IP if not provided
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        self.lock = threading.Lock()
        self.waiting_player = None  # Keeps track of players waiting for a match
        self.total_moves = 0  # For throughput calculation
        self.start_time = time.time()  # To measure throughput over time

    def handle_client(self, player1, player2):
        # Players are connected and ready for a match
        players = [(player1, 'X'), (player2, 'O')]
        current_player_index = 0
        board = [[" " for _ in range(3)] for _ in range(3)]

        def send_message_to_player(player_conn, message):
            player_conn.sendall(message.encode())

        def broadcast_to_players(message):
            for player, _ in players:
                player.sendall(message.encode())

        def draw_grid():
            grid = "\n     1   2   3\n"
            grid += f"  A  {board[0][0]} | {board[0][1]} | {board[0][2]}\n"
            grid += "    ---|---|---\n"
            grid += f"  B  {board[1][0]} | {board[1][1]} | {board[1][2]}\n"
            grid += "    ---|---|---\n"
            grid += f"  C  {board[2][0]} | {board[2][1]} | {board[2][2]}\n"
            return grid

        def is_valid_move(coord):
            if len(coord) != 2 or coord[0] not in '123' or coord[1].upper() not in 'ABC':
                return False
            row, col = coord_to_index(coord)
            return board[row][col] == " "

        def coord_to_index(coord):
            row = ord(coord[1].upper()) - ord('A')
            col = int(coord[0]) - 1
            return row, col

        def did_win(symbol):
            for i in range(3):
                if all(board[i][j] == symbol for j in range(3)):
                    return True
                if all(board[j][i] == symbol for j in range(3)):
                    return True
            if all(board[i][i] == symbol for i in range(3)):
                return True
            if all(board[i][2 - i] == symbol for i in range(3)):
                return True
            return False

        def is_draw():
            return all(board[i][j] != " " for i in range(3) for j in range(3))

        while True:
            current_player, current_symbol = players[current_player_index]
            other_player, _ = players[1 - current_player_index]

            broadcast_to_players(draw_grid())
            send_message_to_player(current_player, "Your move. Enter a coordinate (e.g., 1A): ")

            # Start time for delay measurement
            move_start_time = time.time()

            move = current_player.recv(1024).decode().strip()

            move_end_time = time.time()

            if is_valid_move(move):
                row, col = coord_to_index(move)
                board[row][col] = current_symbol

                # Measure delay (round-trip time) for this move
                move_delay = move_end_time - move_start_time
                print(f"Move delay: {move_delay:.4f} seconds")

                # Update move count for throughput calculation
                self.total_moves += 1
                elapsed_time = time.time() - self.start_time
                throughput = self.total_moves / elapsed_time if elapsed_time > 0 else 0
                print(f"Throughput: {throughput:.2f} moves per second")

                if did_win(current_symbol):
                    broadcast_to_players(f"Player {current_symbol} wins!\n")
                    break

                if is_draw():
                    broadcast_to_players("It's a draw!\n")
                    break

                current_player_index = 1 - current_player_index
            else:
                send_message_to_player(current_player, "Invalid move. Try again.")

        # Close player connections after the match
        player1.close()
        player2.close()

    def start(self):
        print(f"Server started on {self.host}:{self.port}")

        while True:
            conn, addr = self.server_socket.accept()
            print(f"Player connected from {addr}")

            self.lock.acquire()

            if self.waiting_player is None:
                self.waiting_player = conn
                conn.sendall("Waiting for another player...".encode())
            else:
                player1 = self.waiting_player
                player2 = conn
                self.waiting_player = None
                threading.Thread(target=self.handle_client, args=(player1, player2)).start()

            self.lock.release()


if __name__ == "__main__":
    server = TicTacToeServer()
    server.start()
