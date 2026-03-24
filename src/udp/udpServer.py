#!/usr/bin/env python3
"""
Backend UDP file server using Stop-and-Wait ARQ.
Handles:
  - put <filename>
  - get <filename>

All files stored under: uploads/
"""

import os
import sys
import socket

CHUNK_SIZE = 1000
TIMEOUT_S = 1.0


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
    """Receive LEN:N from expected_addr and return N, or None."""
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


def handle_put(sock, addr, filename):
    """Server side of PUT: receive file with Stop-and-Wait and send FIN."""
    filesize = recv_len(sock, addr)
    if filesize is None:
        print("[SERVER] Invalid LEN for PUT.")
        return

    os.makedirs("uploads", exist_ok=True)
    save_path = os.path.join("uploads", filename)

    print(f"[SERVER] Receiving PUT for {filename} ({filesize} bytes)")

    expected_seq = 0
    received = 0

    with open(save_path, "wb") as f:
        while received < filesize:
            sock.settimeout(TIMEOUT_S)
            try:
                pkt, src = sock.recvfrom(65535)
            except socket.timeout:
                print("[SERVER] Timeout waiting for DATA, aborting PUT.")
                return

            if src != addr:
                continue

            seq, payload = parse_data_packet(pkt)
            if seq is None:
                continue

            if seq == expected_seq:
                f.write(payload)
                received += len(payload)
                ack_msg = f"ACK:{seq}".encode()
                print(f"[SERVER] Sending ACK:{seq} to {addr}")
                sock.sendto(ack_msg, addr)
                expected_seq += 1
            else:
                last_good = expected_seq - 1
                ack_msg = f"ACK:{last_good}".encode()
                print(f"[SERVER] Re-sending ACK:{last_good} to {addr}")
                sock.sendto(ack_msg, addr)

    # Finished receiving file
    print(f"[SERVER] PUT complete: {save_path}")
    sock.sendto(b"FIN", addr)


def handle_get(sock, addr, filename):
    """Server side of GET: send file with Stop-and-Wait; expect FIN from client."""
    path = os.path.join("uploads", filename)

    if not os.path.exists(path):
        print(f"[SERVER] GET {filename}: file not found.")
        sock.sendto(b"LEN:0", addr)
        return

    with open(path, "rb") as f:
        data = f.read()

    filesize = len(data)
    print(f"[SERVER] Serving GET for {filename} ({filesize} bytes)")
    sock.sendto(f"LEN:{filesize}".encode(), addr)

    expected_ack = 0
    offset = 0

    while offset < filesize:
        chunk = data[offset:offset + CHUNK_SIZE]
        pkt = b"DATA:%d|" % expected_ack + chunk
        print(f"[SERVER] Sending DATA seq={expected_ack} to {addr}")
        sock.sendto(pkt, addr)

        sock.settimeout(TIMEOUT_S)
        try:
            ack, src = sock.recvfrom(65535)
        except socket.timeout:
            print("[SERVER] Timeout waiting for ACK, resending chunk.")
            continue

        if src != addr or not ack.startswith(b"ACK:"):
            print("[SERVER] Unexpected ACK source or format, ignoring.")
            continue

        try:
            ack_seq = int(ack.decode().split(":")[1])
        except Exception:
            print("[SERVER] Bad ACK format, ignoring.")
            continue

        print(f"[SERVER] Received ACK:{ack_seq} from {addr}")
        if ack_seq == expected_ack:
            offset += CHUNK_SIZE
            expected_ack += 1
        else:
            print("[SERVER] ACK mismatch, resending same chunk.")

    # Wait for FIN from client
    sock.settimeout(TIMEOUT_S)
    try:
        fin, src = sock.recvfrom(65535)
    except socket.timeout:
        print("[SERVER] Timeout waiting for FIN, treating as done.")
        return

    if src == addr and fin == b"FIN":
        print(f"[SERVER] GET complete for {filename}")
    else:
        print("[SERVER] GET finished but FIN mismatch.")


def server_loop(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", port))
    print(f"[SERVER] UDP listening on {port}...")

    while True:
        try:
            data, addr = sock.recvfrom(65535)
        except TimeoutError:
            continue
        cmd = data.decode(errors="ignore").strip().split()

        if not cmd:
            continue

        action = cmd[0].lower()

        if action == "quit":
            print("[SERVER] Quit received, shutting down.")
            break

        elif action == "put" and len(cmd) == 2:
            filename = cmd[1]
            handle_put(sock, addr, filename)

        elif action == "get" and len(cmd) == 2:
            filename = cmd[1]
            handle_get(sock, addr, filename)

        else:
            print(f"[SERVER] Invalid command from {addr}: {data!r}")

    sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 udpServer.py <port>")
        sys.exit(1)
    server_loop(int(sys.argv[1]))
