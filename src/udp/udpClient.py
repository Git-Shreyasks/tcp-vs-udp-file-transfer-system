#!/usr/bin/env python3
"""
UDP Client using Stop-and-Wait ARQ.
Talks ONLY to the CACHE server (which talks to backend).
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


def handle_put(sock, server_addr, filepath):
    if not os.path.exists(filepath):
        print("[CLIENT] File not found.")
        return

    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)

    print(f"[CLIENT] PUT {filename} ({filesize} bytes)")
    sock.sendto(f"put {filename}".encode(), server_addr)
    sock.sendto(f"LEN:{filesize}".encode(), server_addr)

    seq = 0
    sent = 0

    with open(filepath, "rb") as f:
        while sent < filesize:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break

            pkt = b"DATA:%d|" % seq + chunk
            print(f"[CLIENT] Sending DATA seq={seq} to {server_addr}")
            sock.sendto(pkt, server_addr)

            sock.settimeout(TIMEOUT_S)
            try:
                ack, addr = sock.recvfrom(65535)
            except socket.timeout:
                print("[CLIENT] Timeout waiting for ACK, resending.")
                continue

            if addr != server_addr or not ack.startswith(b"ACK:"):
                print("[CLIENT] Unexpected ACK, ignoring.")
                continue

            try:
                ack_seq = int(ack.decode().split(":")[1])
            except Exception:
                print("[CLIENT] Bad ACK format, ignoring.")
                continue

            print(f"[CLIENT] Received ACK:{ack_seq} from {server_addr}")
            if ack_seq == seq:
                sent += len(chunk)
                seq += 1
            else:
                print("[CLIENT] ACK mismatch, resending.")

    # Expect FIN from cache
    sock.settimeout(TIMEOUT_S)
    try:
        fin, addr = sock.recvfrom(65535)
    except socket.timeout:
        print("[CLIENT] Timeout waiting for FIN after PUT.")
        return

    if addr == server_addr and fin == b"FIN":
        print("[CLIENT] PUT complete.")
    else:
        print("[CLIENT] PUT finished but FIN mismatch.")


def handle_get(sock, server_addr, filename):
    print(f"[CLIENT] GET {filename}")
    sock.sendto(f"get {filename}".encode(), server_addr)

    filesize = recv_len(sock, server_addr)
    if filesize is None:
        print("[CLIENT] Invalid LEN from cache.")
        return
    if filesize == 0:
        print("[CLIENT] File not found in cache/server.")
        return

    print(f"[CLIENT] Expecting {filesize} bytes for {filename}")
    os.makedirs("downloads", exist_ok=True)
    save_path = os.path.join("downloads", filename)

    expected_seq = 0
    received = 0

    with open(save_path, "wb") as f:
        while received < filesize:
            sock.settimeout(TIMEOUT_S)
            try:
                pkt, addr = sock.recvfrom(65535)
            except socket.timeout:
                print("[CLIENT] Timeout waiting for DATA, aborting GET.")
                return

            if addr != server_addr:
                continue

            if pkt == b"FIN":
                break

            seq, payload = parse_data_packet(pkt)
            if seq is None:
                continue

            if seq == expected_seq:
                f.write(payload)
                received += len(payload)
                ack_msg = f"ACK:{seq}".encode()
                print(f"[CLIENT] Sending ACK:{seq} to {server_addr}")
                sock.sendto(ack_msg, server_addr)
                expected_seq += 1
            else:
                last_good = expected_seq - 1
                ack_msg = f"ACK:{last_good}".encode()
                print(f"[CLIENT] Re-sending ACK:{last_good} to {server_addr}")
                sock.sendto(ack_msg, server_addr)

    # Send FIN to cache
    sock.sendto(b"FIN", server_addr)
    print(f"[CLIENT] GET complete, saved to {save_path}")


def main(cache_ip, cache_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_addr = (cache_ip, cache_port)

    while True:
        cmdline = input("Enter command (put/get/quit): ").strip()
        if not cmdline:
            continue

        parts = cmdline.split(maxsplit=1)
        action = parts[0].lower()

        if action == "quit":
            sock.sendto(b"quit", server_addr)
            break

        elif action == "put" and len(parts) == 2:
            handle_put(sock, server_addr, parts[1])

        elif action == "get" and len(parts) == 2:
            filename = os.path.basename(parts[1])
            handle_get(sock, server_addr, filename)

        else:
            print("[CLIENT] Invalid command.")

    sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 udpClient.py <cache_ip> <cache_port>")
        sys.exit(1)
    main(sys.argv[1], int(sys.argv[2]))
