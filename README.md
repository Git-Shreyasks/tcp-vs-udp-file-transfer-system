# TCP vs UDP File Transfer System
1)Implemented and evaluated TCP vs UDP file transfer protocols with performance benchmarking and system-level analysis
2)A systems-level networking project that implements and evaluates reliable (TCP) and unreliable (UDP) file transfer protocols, highlighting trade-offs between    reliability, latency, and throughput.

##  Motivation

Modern distributed systems rely heavily on efficient data transfer. While TCP guarantees reliability, UDP offers lower latency but no delivery guarantees.

This project explores:

* When reliability is necessary
* When speed is more important
* How protocol design impacts performance

##  Key Concepts

* Reliable Data Transfer (RDT)
* Acknowledgment & Retransmission
* Packet Loss Handling
* Throughput vs Latency Trade-offs
* Connection-oriented vs Connectionless Communication

##  System Architecture

### TCP Mode

* Connection-oriented communication
* Guaranteed delivery
* Ordered packets
* Built-in congestion control

### UDP Mode

* Connectionless communication
* No delivery guarantees
* Faster transmission
* Custom handling required (if reliability needed)


##  Features

* File transfer using TCP and UDP
* Modular client-server implementation
* Performance comparison framework
* Extensible design for experimentation


##  Tech Stack

* Python
* Socket Programming
* TCP/UDP Networking


##  Experimental Analysis - structure

| Protocol | Transfer Time (s) | Reliability |
| -------- | ----------------- | ----------- |
| TCP      |                   | %        |
| UDP      |                   | %         |

### Observations

* TCP ensures reliability but introduces overhead
* UDP is faster but may result in packet loss
* Trade-offs depend on application (e.g., streaming vs file transfer)

##  How to Run

### TCP

```bash
python src/tcp/server.py
python src/tcp/client.py
```

### UDP

```bash
python src/udp/server.py
python src/udp/client.py
```

## Applications

* Distributed systems communication
* Real-time streaming systems (UDP)
* Reliable file transfer systems (TCP)
* Network protocol experimentation
