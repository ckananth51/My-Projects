import socket
import threading
import json
import os
import time
import random

class RaftNode:
    def __init__(self, id, port, peers):
        self.id = id
        self.port = port
        self.peers = peers
        self.log = self.load_log()
        self.connections = []

        self.state = "follower"
        self.current_term = self.load_state("term")
        self.voted_for = self.load_state("vote")
        self.votes_received = 0
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(3, 5)

        self.leader_address = None
        self.lock = threading.Lock()

    def load_log(self):
        try:
            with open(f"log_{self.id}.json", "r") as f:
                return json.load(f)
        except:
            return []

    def save_log(self):
        with open(f"log_{self.id}.json", "w") as f:
            json.dump(self.log, f)

    def load_state(self, kind):
        try:
            with open(f"state_{self.id}.json", "r") as f:
                state = json.load(f)
                return state.get(kind, None)
        except:
            return 0 if kind == "term" else None

    def save_state(self):
        with open(f"state_{self.id}.json", "w") as f:
            json.dump({"term": self.current_term, "vote": self.voted_for}, f)

    def start(self):
        # Small delay to avoid simultaneous startup collisions
        time.sleep(3 + self.id)
        threading.Thread(target=self.accept_clients, daemon=True).start()
        threading.Thread(target=self.listen_for_replication, daemon=True).start()
        threading.Thread(target=self.check_heartbeat_timeout, daemon=True).start()

        print(f"[{self.state.capitalize()}] Node {self.id} started on port {self.port}")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[Node {self.id}] Shutting down.")

    def accept_clients(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', self.port))
        s.listen(5)
        while True:
            conn, addr = s.accept()
            self.connections.append(conn)
            threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()

    def handle_client(self, conn):
        while True:
            try:
                data = conn.recv(4096).decode()
                if data:
                    msg = json.loads(data)
                    if self.state == "leader":
                        print(f"[Received] {msg['message']}")
                        self.log.append(msg['message'])
                        self.save_log()
                        self.replicate_log()
                        self.broadcast_to_clients(msg['message'])
                        conn.sendall(f"[Chat] {msg['message']}\n".encode())
                    else:
                        print("[Forwarding] Message to leader")
                        if self.leader_address:
                            self.forward_to_leader(conn, data)
                        else:
                            conn.sendall(b"[Error] No leader known.\n")
                else:
                    break
            except Exception as e:
                print(f"[Error] {e}")
                break

    def forward_to_leader(self, client_conn, data):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(self.leader_address)
                s.sendall(data.encode())
                response = s.recv(4096)
                client_conn.sendall(response)
        except Exception as e:
            print(f"[Error forwarding to leader]: {e}")
            try:
                client_conn.sendall(b"[Error] Failed to reach leader.\n")
            except:
                pass

    def replicate_log(self):
        entry = json.dumps({
            "type": "append_entries",
            "term": self.current_term,
            "log": self.log,
            "leader_port": self.port
        })
        for peer in self.peers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ps:
                    ps.connect((peer[0], peer[1] + 1000))
                    ps.send(entry.encode())
            except:
                pass

    def broadcast_to_clients(self, message):
        # Send a message to all clients connected to this node
        for conn in self.connections:
            try:
                conn.sendall(f"[Chat] {message}\n".encode())
            except:
                pass

    def listen_for_replication(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', self.port + 1000))
        s.listen(5)
        while True:
            conn, addr = s.accept()
            threading.Thread(target=self.handle_peer, args=(conn,), daemon=True).start()

    def handle_peer(self, conn):
        try:
            data = conn.recv(4096).decode()
            if not data:
                return
            msg = json.loads(data)

            if msg["type"] == "append_entries":
                with self.lock:
                    if msg.get("term", 0) >= self.current_term:
                        leader_client_port = msg.get("leader_port", self.port)
                        new_leader_address = ('localhost', leader_client_port)
                        
                        if self.leader_address != new_leader_address:
                            print(f"[Update] Node {self.id} recognizes {new_leader_address} as new leader")
                        
                        self.state = "follower"
                        self.current_term = msg["term"]
                        self.voted_for = None
                        self.leader_address = new_leader_address
                        self.save_state()
                    
                    self.last_heartbeat = time.time()

                # ----------- NEW LOGIC TO BROADCAST APPENDED MESSAGES -----------
                if "log" in msg:
                    # Check if the incoming log is longer than our current log
                    if len(msg["log"]) > len(self.log):
                        old_length = len(self.log)
                        self.log = msg["log"]
                        self.save_log()
                        print("[Log Sync] Full chat log after recovery:")
                        for m in self.log:
                            print(f"[Chat] {m}")

                        # Identify newly appended messages
                        appended_messages = self.log[old_length:]
                        # Broadcast them to all clients connected to this follower
                        for new_msg in appended_messages:
                            self.broadcast_to_clients(new_msg)

            elif msg["type"] == "request_vote":
                vote_granted = False
                with self.lock:
                    if msg["term"] > self.current_term:
                        self.current_term = msg["term"]
                        self.voted_for = None
                        self.state = "follower"
                        self.save_state()

                    if self.voted_for in (None, msg["candidate_id"]) and msg["term"] >= self.current_term:
                        vote_granted = True
                        self.voted_for = msg["candidate_id"]
                        self.last_heartbeat = time.time()
                        self.save_state()
                        print(f"[Vote Granted] Node {self.id} voted for Node {msg['candidate_id']}")

                response = json.dumps({
                    "type": "vote_response",
                    "vote_granted": vote_granted,
                    "term": self.current_term
                })
                try:
                    conn.sendall(response.encode())
                except:
                    pass
        except Exception as e:
            print(f"[Error - handle_peer]: {e}")

    def send_heartbeats(self):
        while self.state == "leader":
            entry = json.dumps({
                "type": "append_entries",
                "term": self.current_term,
                "leader_port": self.port
            })
            for peer in self.peers:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((peer[0], peer[1] + 1000))
                        s.send(entry.encode())
                except:
                    pass
            time.sleep(2)

    def check_heartbeat_timeout(self):
        while True:
            time.sleep(1)
            if time.time() - self.last_heartbeat > self.election_timeout and self.state != "leader":
                print(f"[Timeout] Node {self.id} starting election")
                self.start_election()

    def start_election(self):
        with self.lock:
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.id
            self.votes_received = 1
            self.last_heartbeat = time.time()
            self.leader_address = ('localhost', self.port)
            self.save_state()

        print(f"[Election] Node {self.id} requesting votes (term {self.current_term})")

        request = json.dumps({
            "type": "request_vote",
            "term": self.current_term,
            "candidate_id": self.id
        })

        def request_vote(peer):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2)
                    s.connect((peer[0], peer[1] + 1000))
                    s.send(request.encode())
                    response = s.recv(4096).decode()
                    if response:
                        msg = json.loads(response)
                        if msg.get("vote_granted") and self.state == "candidate":
                            with self.lock:
                                self.votes_received += 1
                                print(f"[Vote Received] Node {self.id} now has {self.votes_received} votes")
                                if self.votes_received > (len(self.peers) + 1) // 2:
                                    print(f"[Election] Node {self.id} becomes leader (term {self.current_term})")
                                    self.state = "leader"
                                    self.leader_address = ('localhost', self.port)
                                    self.save_state()
                                    threading.Thread(target=self.send_heartbeats, daemon=True).start()
            except:
                pass

        for peer in self.peers:
            threading.Thread(target=request_vote, args=(peer,), daemon=True).start()


if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    peers = [('localhost', int(p)) for p in sys.argv[3].split(',')] if len(sys.argv) > 3 else []
    node = RaftNode(node_id, port, peers)
    node.start()
