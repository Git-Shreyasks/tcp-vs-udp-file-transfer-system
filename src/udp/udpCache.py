#!/usr/bin/env python3
"""
Middle-layer UDP Cache Server.
Client <--> CACHE <--> Backend udpServer.py

Implements Stop-and-Wait independently on:
  - client <-> cache
  - cache <-> server

NOW WITH:
  - Persistent disk cache in client_cache/
  - Auto-load cache entries on startup
  - Auto-save cache entries after PUT and GET MISS
"""

import sys
import socket
import os

CHUNK_SIZE = 1000
TIMEOUT_S = 1.0

# ============================================================
# Persistent disk-based cache initialization
# ============================================================

CACHE_DIR = "client_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# In-memory cache: filename -> bytes
CACHE = {}

print("[CACHE] Preloading cache from disk...")
for fname in os.listdir(CACHE_DIR):
    full = os.path.join(CACHE_DIR, fname)
    if os.path.isfile(full):
        with open(full, "rb") as f:
            CACHE[fname] = f.read()
        print(f"[CACHE]   loaded: {fname} ({len(CACHE[fname])} bytes)")
print("[CACHE] Preload complete.\n")


# ============================================================
# Packet parsing helpers
# ============================================================

def parse_data_packet(packet: bytes):
    if not packet.startswith(b"DATA:"):
        return None, None
    try:
        head, payload = packet.split(b"|", 1)
        seq = int(head.decode().split(":")[1])
        return seq, payload
    except Exception:
        return None, None


def recv_len(sock, expected_addr):
    sock.settimeout(TIMEOUT_S)
    try:
        data, addr = sock.recvfrom(65535)
    except socket.timeout:
        return None

    if addr != expected_addr or not data.startswith(b"LEN:"):
        return None

    try:
        return int(data.decode().split(":")[1])
    except Exception:
        return None


# ============================================================
# Backend (cache <-> server) GET handling
# ============================================================

def fetch_from_server(sock, server_addr, filename):
    print(f"[CACHE] MISS: fetching {filename} from backend {server_addr}")
    sock.sendto(f"get {filename}".encode(), server_addr)

    filesize = recv_len(sock, server_addr)
    if filesize is None or filesize == 0:
        print("[CACHE] Backend missing or invalid LEN.")
        return None

    expected_seq = 0
    received = 0
    buf = b""

    while received < filesize:
        sock.settimeout(TIMEOUT_S)
        try:
            pkt, addr = sock.recvfrom(65535)
        except socket.timeout:
            print("[CACHE] Timeout waiting for DATA from backend, aborting GET.")
            return None

        if addr != server_addr:
            continue

        seq, payload = parse_data_packet(pkt)
        if seq is None:
            continue

        if seq == expected_seq:
            buf += payload
            received += len(payload)
            ack_msg = f"ACK:{seq}".encode()
            print(f"[CACHE] (backend) Sending ACK:{seq}")
            sock.sendto(ack_msg, server_addr)
            expected_seq += 1
        else:
            last_good = expected_seq - 1
            ack_msg = f"ACK:{last_good}".encode()
            print(f"[CACHE] (backend) Re-ACK:{last_good}")
            sock.sendto(ack_msg, server_addr)

    print(f"[CACHE] Finished fetching {filename} ({len(buf)} bytes).")
    sock.sendto(b"FIN", server_addr)
    return buf


# ============================================================
# Backend PUT (cache -> server)
# ============================================================

def send_file_to_server(sock, server_addr, filename, file_bytes):
    filesize = len(file_bytes)
    print(f"[CACHE] Forwarding PUT {filename} ({filesize} bytes) to backend")

    sock.sendto(f"put {filename}".encode(), server_addr)
    sock.sendto(f"LEN:{filesize}".encode(), server_addr)

    expected_ack = 0
    offset = 0

    while offset < filesize:
        chunk = file_bytes[offset:offset + CHUNK_SIZE]
        pkt = b"DATA:%d|" % expected_ack + chunk
        print(f"[CACHE] (backend) Sending DATA seq={expected_ack}")
        sock.sendto(pkt, server_addr)

        sock.settimeout(TIMEOUT_S)
        try:
            ack, addr = sock.recvfrom(65535)
        except socket.timeout:
            print("[CACHE] Timeout waiting for backend ACK, resending.")
            continue

        if addr != server_addr or not ack.startswith(b"ACK:"):
            continue

        ack_seq = int(ack.decode().split(":")[1])
        print(f"[CACHE] (backend) Received ACK:{ack_seq}")

        if ack_seq == expected_ack:
            offset += CHUNK_SIZE
            expected_ack += 1

    # Wait for FIN
    sock.settimeout(TIMEOUT_S)
    try:
        fin, addr = sock.recvfrom(65535)
    except socket.timeout:
        print("[CACHE] Timeout waiting for FIN after PUT.")
        return

    if addr == server_addr and fin == b"FIN":
        print(f"[CACHE] PUT to backend complete for {filename}")
    else:
        print("[CACHE] FIN mismatch after PUT.")


# ============================================================
# Client GET (cache -> client)
# ============================================================

def send_file_to_client(sock, client_addr, file_bytes):
    filesize = len(file_bytes)
    print(f"[CACHE] Sending file ({filesize} bytes) to client {client_addr}")
    sock.sendto(f"LEN:{filesize}".encode(), client_addr)

    expected_ack = 0
    offset = 0

    while offset < filesize:
        chunk = file_bytes[offset:offset + CHUNK_SIZE]
        pkt = b"DATA:%d|" % expected_ack + chunk
        print(f"[CACHE] (client) Sending DATA seq={expected_ack}")
        sock.sendto(pkt, client_addr)

        sock.settimeout(TIMEOUT_S)
        try:
            ack, addr = sock.recvfrom(65535)
        except socket.timeout:
            print("[CACHE] Resending DATA due to timeout.")
            continue

        if addr != client_addr or not ack.startswith(b"ACK:"):
            continue

        ack_seq = int(ack.decode().split(":")[1])
        print(f"[CACHE] (client) Received ACK:{ack_seq}")

        if ack_seq == expected_ack:
            offset += CHUNK_SIZE
            expected_ack += 1

    # Wait for FIN from client
    sock.settimeout(TIMEOUT_S)
    try:
        fin, addr = sock.recvfrom(65535)
    except socket.timeout:
        print("[CACHE] Timeout waiting for FIN from client.")
        return

    if addr == client_addr and fin == b"FIN":
        print("[CACHE] Finished sending file to client.")
    else:
        print("[CACHE] FIN mismatch after GET.")


# ============================================================
# Client PUT (cache receives)
# ============================================================

def receive_file_from_client(sock, client_addr, filename):
    filesize = recv_len(sock, client_addr)
    if filesize is None:
        print("[CACHE] Invalid LEN from client in PUT.")
        return None

    print(f"[CACHE] Receiving PUT for {filename} ({filesize} bytes)")
    expected_seq = 0
    received = 0
    buf = b""

    while received < filesize:
        sock.settimeout(TIMEOUT_S)
        try:
            pkt, addr = sock.recvfrom(65535)
        except socket.timeout:
            print("[CACHE] Timeout waiting for DATA from client.")
            return None

        if addr != client_addr:
            continue

        seq, payload = parse_data_packet(pkt)
        if seq is None:
            continue

        if seq == expected_seq:
            buf += payload
            received += len(payload)
            sock.sendto(f"ACK:{seq}".encode(), client_addr)
            expected_seq += 1
        else:
            sock.sendto(f"ACK:{expected_seq-1}".encode(), client_addr)

    print(f"[CACHE] Completed receive of {filename}.")
    sock.sendto(b"FIN", client_addr)
    return buf


# ============================================================
# MAIN LOOP
# ============================================================

def cache_loop(cache_port, backend_ip, backend_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", cache_port))
    server_addr = (backend_ip, backend_port)

    print(f"[CACHE] Listening on {cache_port}, backend at {server_addr}")

    while True:
        try:
            data, client_addr = sock.recvfrom(65535)
        except socket.timeout:
            continue

        cmd = data.decode(errors="ignore").strip().split()
        if not cmd:
            continue

        action = cmd[0].lower()

        # QUIT
        if action == "quit":
            print("[CACHE] Quit received, shutting down.")
            sock.sendto(b"quit", server_addr)
            break

        # ========================================================
        # GET
        # ========================================================
        elif action == "get" and len(cmd) == 2:
            filename = cmd[1]

            if filename in CACHE:
                print(f"[CACHE] HIT for {filename}")
                file_bytes = CACHE[filename]
            else:
                print(f"[CACHE] MISS for {filename}")
                file_bytes = fetch_from_server(sock, server_addr, filename)
                if file_bytes is None:
                    sock.sendto(b"LEN:0", client_addr)
                    continue

                CACHE[filename] = file_bytes
                with open(os.path.join(CACHE_DIR, filename), "wb") as f:
                    f.write(file_bytes)
                print(f"[CACHE] Stored {filename} in disk cache.")

            send_file_to_client(sock, client_addr, file_bytes)

        # ========================================================
        # PUT
        # ========================================================
        elif action == "put" and len(cmd) == 2:
            filename = cmd[1]

            file_bytes = receive_file_from_client(sock, client_addr, filename)
            if file_bytes is None:
                continue

            CACHE[filename] = file_bytes

            with open(os.path.join(CACHE_DIR, filename), "wb") as f:
                f.write(file_bytes)
            print(f"[CACHE] Saved {filename} to disk cache.")

            send_file_to_server(sock, server_addr, filename, file_bytes)

        else:
            print(f"[CACHE] Invalid command: {data!r}")

    sock.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 udpCache.py <cache_port> <server_ip> <server_port>")
        sys.exit(1)

    cache_port = int(sys.argv[1])
    backend_ip = sys.argv[2]
    backend_port = int(sys.argv[3])

    cache_loop(cache_port, backend_ip, backend_port)
