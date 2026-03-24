#!/usr/bin/env python3
"""
TCP Client.

Talks ONLY to the TCP CACHE server.
"""

import os
import sys
import socket
from typing import Optional, Tuple

CHUNK_SIZE = 1000


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


def handle_put(conn: socket.socket, filepath: str) -> None:
    if not os.path.exists(filepath):
        print("[CLIENT] File not found.")
        return

    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)

    print(f"[CLIENT] PUT {filename} ({filesize} bytes)")
    # send command line
    conn.sendall(f"put {filename}\n".encode())
    # send LEN
    conn.sendall(f"LEN:{filesize}\n".encode())

    with open(filepath, "rb") as f:
        remaining = filesize
        while remaining > 0:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            conn.sendall(chunk)
            remaining -= len(chunk)

    print("[CLIENT] PUT complete.")


def handle_get(conn: socket.socket, filename: str) -> None:
    print(f"[CLIENT] GET {filename}")
    conn.sendall(f"get {filename}\n".encode())

    len_line = recv_line(conn)
    if not len_line:
        print("[CLIENT] Connection closed while waiting for LEN.")
        return

    try:
        text = len_line.decode().strip()
        assert text.startswith("LEN:")
        filesize = int(text.split(":", 1)[1])
    except Exception:
        print(f"[CLIENT] Invalid LEN from cache: {len_line!r}")
        return

    if filesize == 0:
        print("[CLIENT] File not found in cache/server.")
        return

    print(f"[CLIENT] Expecting {filesize} bytes for {filename}")
    os.makedirs("downloads", exist_ok=True)
    save_path = os.path.join("downloads", filename)

    remaining = filesize
    with open(save_path, "wb") as f:
        while remaining > 0:
            to_read = min(CHUNK_SIZE, remaining)
            data = recv_exact(conn, to_read)
            if data is None:
                print("[CLIENT] Connection closed mid-file.")
                return
            f.write(data)
            remaining -= len(data)

    print(f"[CLIENT] GET complete, saved to {save_path}")


def main(cache_ip: str, cache_port: int) -> None:
    while True:
        # For each session we open a TCP connection to cache.
        # You can also keep one persistent connection if you want.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cache_addr: Tuple[str, int] = (cache_ip, cache_port)
        sock.connect(cache_addr)

        try:
            while True:
                cmdline = input("Enter command (put/get/quit): ").strip()
                if not cmdline:
                    continue

                parts = cmdline.split(maxsplit=1)
                action = parts[0].lower()

                if action == "quit":
                    sock.sendall(b"quit\n")
                    return  # exit client program

                elif action == "put" and len(parts) == 2:
                    handle_put(sock, parts[1])

                elif action == "get" and len(parts) == 2:
                    filename = os.path.basename(parts[1])
                    handle_get(sock, filename)

                else:
                    print("[CLIENT] Invalid command.")
        finally:
            sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 tcpClient.py <cache_ip> <cache_port>")
        sys.exit(1)
    main(sys.argv[1], int(sys.argv[2]))
