#!/usr/bin/env python3
"""
Backend TCP file server.

Handles:
  - put <filename>
  - get <filename>
  - quit

All files stored under: uploads/
"""

import os
import sys
import socket
from typing import Optional, Tuple

CHUNK_SIZE = 1000  # for reading/writing files on disk


def recv_line(conn: socket.socket) -> Optional[bytes]:
    """
    Read a line from the TCP stream (ending with b'\\n').
    Returns None if connection closes before a newline is seen.
    """
    buf = bytearray()
    while True:
        ch = conn.recv(1)
        if not ch:
            # Connection closed
            if buf:
                return bytes(buf)
            return None
        buf.extend(ch)
        if ch == b"\n":
            return bytes(buf)


def recv_exact(conn: socket.socket, n: int) -> Optional[bytes]:
    """
    Receive exactly n bytes from conn, or None if connection closes early.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def handle_put(conn: socket.socket, filename: str) -> None:
    """
    Server side of PUT: receive file over TCP and store into uploads/.
    Protocol:
      client -> "LEN:<filesize>\\n"
      then <filesize> raw bytes
    """
    # Read LEN line
    len_line = recv_line(conn)
    if not len_line:
        print("[SERVER] Connection closed while expecting LEN.")
        return

    try:
        text = len_line.decode().strip()
        assert text.startswith("LEN:")
        filesize = int(text.split(":", 1)[1])
    except Exception:
        print(f"[SERVER] Bad LEN line: {len_line!r}")
        return

    os.makedirs("uploads", exist_ok=True)
    save_path = os.path.join("uploads", filename)

    print(f"[SERVER] Receiving PUT for {filename} ({filesize} bytes)")

    remaining = filesize
    with open(save_path, "wb") as f:
        while remaining > 0:
            to_read = min(CHUNK_SIZE, remaining)
            data = recv_exact(conn, to_read)
            if data is None:
                print("[SERVER] Connection closed mid-file during PUT.")
                return
            f.write(data)
            remaining -= len(data)

    print(f"[SERVER] PUT complete: {save_path}")


def handle_get(conn: socket.socket, filename: str) -> None:
    """
    Server side of GET: send file over TCP.
    Protocol:
      server -> "LEN:<filesize>\\n"
      then <filesize> raw bytes
    If file missing:
      server -> "LEN:0\\n"
    """
    path = os.path.join("uploads", filename)

    if not os.path.exists(path):
        print(f"[SERVER] GET {filename}: file not found.")
        conn.sendall(b"LEN:0\n")
        return

    with open(path, "rb") as f:
        data = f.read()

    filesize = len(data)
    print(f"[SERVER] Serving GET for {filename} ({filesize} bytes)")

    header = f"LEN:{filesize}\n".encode()
    conn.sendall(header)

    offset = 0
    while offset < filesize:
        chunk = data[offset:offset + CHUNK_SIZE]
        conn.sendall(chunk)
        offset += len(chunk)


def handle_client(conn: socket.socket, addr: Tuple[str, int]) -> None:
    """
    Handle commands from a single client connection.
    """
    print(f"[SERVER] New TCP connection from {addr}")
    try:
        while True:
            line = recv_line(conn)
            if not line:
                print(f"[SERVER] Connection {addr} closed by client.")
                break

            cmd = line.decode(errors="ignore").strip().split()
            if not cmd:
                continue

            action = cmd[0].lower()

            if action == "quit":
                print(f"[SERVER] Quit from {addr}, closing connection.")
                break

            elif action == "put" and len(cmd) == 2:
                filename = cmd[1]
                handle_put(conn, filename)

            elif action == "get" and len(cmd) == 2:
                filename = cmd[1]
                handle_get(conn, filename)

            else:
                print(f"[SERVER] Invalid command from {addr}: {line!r}")
    finally:
        conn.close()
        print(f"[SERVER] Closed connection to {addr}")


def server_loop(port: int) -> None:
    srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv_sock.bind(("", port))
    srv_sock.listen(5)
    print(f"[SERVER] TCP listening on port {port}...")

    try:
        while True:
            conn, addr = srv_sock.accept()
            # Simple single-threaded handling; for more concurrency,
            # you could spawn a thread per client.
            handle_client(conn, addr)
    finally:
        srv_sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 tcpServer.py <port>")
        sys.exit(1)
    server_loop(int(sys.argv[1]))
