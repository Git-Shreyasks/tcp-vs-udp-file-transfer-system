# TCP vs UDP File Transfer System

A systems-level networking project that implements and compares reliable (TCP-based) and unreliable (UDP-based) file transfer mechanisms using Python socket programming.

1)Implemented and evaluated TCP vs UDP file transfer protocols with performance benchmarking and system-level analysis


## Overview

This project explores the fundamental differences between TCP and UDP by building a file transfer system for both protocols and analyzing their behavior under different conditions.

The goal was to understand how reliability, ordering, and performance vary across protocols, and how design choices impact real-world systems.


##  Key Concepts Covered

* Reliable vs Unreliable Data Transfer
* Connection-Oriented (TCP) vs Connectionless (UDP) Communication
* Packet Loss and Data Integrity
* Acknowledgment and Retransmission
* Throughput vs Latency Trade-offs

---

##  Project Structure

```
tcp-vs-udp-file-transfer-system/
│── src/
│   ├── tcp/
│   │   ├── tcpServer.py
│   │   ├── tcpClient.py
│   │   ├── tcpCache.py
│   ├── udp/
│   │   ├── udpServer.py
│   │   ├── udpClient.py
│   │   ├── udpCache.py
│── experiments/
│   ├── tcp_e/
│   ├── udp_e/
│── README.md
│── requirements.txt
```

---

##  Implementation Details

###  TCP Implementation

* Uses connection-oriented sockets
* Ensures reliable delivery of files
* Maintains ordering of packets
* Handles complete file reconstruction without loss

###  UDP Implementation

* Uses connectionless sockets
* Faster transmission with minimal overhead
* No guarantee of delivery or order
* Demonstrates packet loss scenarios

###  Caching Layer

* Implemented caching for transferred files
* Improves efficiency for repeated transfers
* Reduces redundant network usage

---

##  Experiments

The project includes experimental setups for both TCP and UDP under:

* Upload scenarios
* Download scenarios
* Client cache usage

Located in:

```
experiments/tcp_e/
experiments/udp_e/
```

---

##  Observations

* TCP provides **100% reliability** but introduces overhead due to acknowledgments and retransmissions
* UDP achieves **lower latency** but may result in packet loss
* For applications like file transfer, TCP is more suitable
* For real-time systems (e.g., streaming), UDP is often preferred

---

##  How to Run

###  TCP

Start server:

```bash
python src/tcp/tcpServer.py
```

Start client:

```bash
python src/tcp/tcpClient.py
```

---

###  UDP

Start server:

```bash
python src/udp/udpServer.py
```

Start client:

```bash
python src/udp/udpClient.py
```

---

##  Tech Stack

* Python
* Socket Programming
* TCP/UDP Protocols

---

##  Design Trade-offs

| Aspect      | TCP        | UDP            |
| ----------- | ---------- | -------------- |
| Reliability | High       | Low            |
| Speed       | Moderate   | High           |
| Ordering    | Guaranteed | Not Guaranteed |
| Overhead    | High       | Low            |

---

##  Applications

* Distributed Systems Communication
* File Transfer Systems
* Real-time Streaming Systems
* Network Protocol Research

---

Developed as part of a team project and later refactored and structured for clarity and presentation.

* Shreyas K S
* Teammate(s)

