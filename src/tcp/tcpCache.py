#!/usr/bin/env python3
"""
Middle-layer TCP Cache Server.
Client <--> CACHE <--> Backend tcpServer.py

Implements a simple persistent cache:
  - client_cache/ on disk
  - in-memory dict: filename -> bytes
"""

import sys
import socket
import os
from typing import Optional, Tuple

CHUNK_SIZE = 1000

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


def recv_line(conn: socket.socket) -> Optional[bytes]:
    buf = bytearray()
    while True:
        ch = conn.recv(1)
        if not ch:
            if buf:
                return bytes(buf)
            return None
        buf.extend(ch)
        if ch == b"\n":
            return bytes(buf)


def recv_exact(conn: socket.socket, n: int) -> Optional[bytes]:
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


# ============================================================
# Backend helpers (cache <-> backend server)
# ============================================================

def backend_get(backend_addr: Tuple[str, int], filename: str) -> Optional[bytes]:
    """
    Fetch file from backend server over TCP.
    Returns file bytes or None if error / not found.
    """
    print(f"[CACHE] MISS: fetching {filename} from backend {backend_addr}")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(backend_addr)

        # send GET command
        cmd = f"get {filename}\n".encode()
        s.sendall(cmd)

        # read LEN line
        len_line = recv_line(s)
        if not len_line:
            print("[CACHE] Backend closed connection without LEN.")
            return None

        try:
            text = len_line.decode().strip()
            assert text.startswith("LEN:")
            filesize = int(text.split(":", 1)[1])
        except Exception:
            print(f"[CACHE] Backend invalid LEN: {len_line!r}")
            return None

        if filesize == 0:
            print("[CACHE] Backend reports file not found.")
            return None

        remaining = filesize
        buf = bytearray()
        while remaining > 0:
            to_read = min(CHUNK_SIZE, remaining)
            data = recv_exact(s, to_read)
            if data is None:
                print("[CACHE] Backend closed mid-file.")
                return None
            buf.extend(data)
            remaining -= len(data)

        print(f"[CACHE] Finished fetching {filename} ({len(buf)} bytes) from backend.")
        return bytes(buf)

    finally:
        s.close()


def backend_put(backend_addr: Tuple[str, int], filename: str, file_bytes: bytes) -> None:
    """
    Forward a PUT to backend server over TCP.
    """
    filesize = len(file_bytes)
    print(f"[CACHE] Forwarding PUT {filename} ({filesize} bytes) to backend {backend_addr}")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(backend_addr)
        # send PUT command
        cmd = f"put {filename}\n".encode()
        s.sendall(cmd)
        # send LEN
        s.sendall(f"LEN:{filesize}\n".encode())
        # send data
        offset = 0
        while offset < filesize:
            chunk = file_bytes[offset:offset + CHUNK_SIZE]
            s.sendall(chunk)
            offset += len(chunk)
        print(f"[CACHE] PUT to backend complete for {filename}")
    finally:
        s.close()


# ============================================================
# Client-facing helpers (cache <-> client)
# ============================================================

def send_file_to_client(conn: socket.socket, file_bytes: bytes) -> None:
    filesize = len(file_bytes)
    conn.sendall(f"LEN:{filesize}\n".encode())
    offset = 0
    while offset < filesize:
        chunk = file_bytes[offset:offset + CHUNK_SIZE]
        conn.sendall(chunk)
        offset += len(chunk)


def recv_file_from_client(conn: socket.socket, filename: str) -> Optional[bytes]:
    len_line = recv_line(conn)
    if not len_line:
        print("[CACHE] Client closed while expecting LEN.")
        return None

    try:
        text = len_line.decode().strip()
        assert text.startswith("LEN:")
        filesize = int(text.split(":", 1)[1])
    except Exception:
        print(f"[CACHE] Invalid LEN from client: {len_line!r}")
        return None

    print(f"[CACHE] Receiving PUT for {filename} ({filesize} bytes) from client")
    remaining = filesize
    buf = bytearray()

    while remaining > 0:
        to_read = min(CHUNK_SIZE, remaining)
        data = recv_exact(conn, to_read)
        if data is None:
            print("[CACHE] Client closed mid-file.")
            return None
        buf.extend(data)
        remaining -= len(data)

    print(f"[CACHE] Completed receive of {filename} from client.")
    return bytes(buf)


# ============================================================
# Main per-client handler
# ============================================================

def handle_client(conn: socket.socket, addr: Tuple[str, int], backend_addr: Tuple[str, int]) -> None:
    print(f"[CACHE] New client connection from {addr}")
    try:
        while True:
            line = recv_line(conn)
            if not line:
                print(f"[CACHE] Client {addr} closed connection.")
                break

            cmd = line.decode(errors="ignore").strip().split()
            if not cmd:
                continue

            action = cmd[0].lower()

            # QUIT
            if action == "quit":
                print(f"[CACHE] Quit from {addr}, closing connection.")
                break

            # GET
            elif action == "get" and len(cmd) == 2:
                filename = cmd[1]

                if filename in CACHE:
                    print(f"[CACHE] HIT for {filename}")
                    file_bytes = CACHE[filename]
                else:
                    print(f"[CACHE] MISS for {filename}")
                    file_bytes = backend_get(backend_addr, filename)
                    if file_bytes is None:
                        conn.sendall(b"LEN:0\n")
                        continue

                    CACHE[filename] = file_bytes
                    with open(os.path.join(CACHE_DIR, filename), "wb") as f:
                        f.write(file_bytes)
                    print(f"[CACHE] Stored {filename} in disk cache.")

                send_file_to_client(conn, file_bytes)

            # PUT
            elif action == "put" and len(cmd) == 2:
                filename = cmd[1]

                file_bytes = recv_file_from_client(conn, filename)
                if file_bytes is None:
                    continue

                # Update cache + disk
                CACHE[filename] = file_bytes
                with open(os.path.join(CACHE_DIR, filename), "wb") as f:
                    f.write(file_bytes)
                print(f"[CACHE] Saved {filename} to disk cache.")

                # Forward to backend
                backend_put(backend_addr, filename, file_bytes)

            else:
                print(f"[CACHE] Invalid command from {addr}: {line!r}")
    finally:
        conn.close()
        print(f"[CACHE] Closed connection to {addr}")


def cache_loop(cache_port: int, backend_ip: str, backend_port: int) -> None:
    backend_addr = (backend_ip, backend_port)

    srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv_sock.bind(("", cache_port))
    srv_sock.listen(5)

    print(f"[CACHE] Listening on {cache_port}, backend at {backend_addr}")
    try:
        while True:
            conn, addr = srv_sock.accept()
            # Single-threaded; can add threading if needed.
            handle_client(conn, addr, backend_addr)
    finally:
        srv_sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 tcpCache.py <cache_port> <server_ip> <server_port>")
        sys.exit(1)

    cache_port = int(sys.argv[1])
    backend_ip = sys.argv[2]
    backend_port = int(sys.argv[3])

    cache_loop(cache_port, backend_ip, backend_port)
