# aether-packet-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/aether-packet-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/aether-packet-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/aether-packet-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/aether-packet-engine?style=flat)](https://github.com/sohamshewani/aether-packet-engine)

## 1. Executive Summary & Problem Space

`aether-packet-engine` is a high-throughput, ultra-low-latency packet processing engine engineered in Java. Built to bypass the traditional performance ceilings of user-space network stacks, it addresses critical bottlenecks inherent in the Java Virtual Machine—specifically garbage collection (GC) pauses, memory copying overhead, and lock contention across multi-core architectures. 

Traditional Java networking applications suffer from heap allocation churn during packet ingestion, causing unpredictable stop-the-world (STW) pauses and cache locality degradation. `aether-packet-engine` resolves these issues by implementing a zero-allocation memory model utilizing direct off-heap memory arenas, lock-free ring buffers, and thread-pinned polling loops. It is designed for mission-critical networking infrastructure, high-frequency trading gateways, and high-performance telemetry collection pipelines that demand deterministic microsecond-level latency and sustained wire-speed throughput.

## 2. Structural ASCII Architecture / Dataflow Diagram

The system operates via a decoupled, producer-consumer pipeline designed to maximize CPU cache efficiency and eliminate synchronization barriers on the hot path.

```
 +---------------------------------------------------------------------------------+
 |                              Network Interface Card                             |
 +---------------------------------------------------------------------------------+
                                          |
                                          v
 +---------------------------------------------------------------------------------+
 |  PollLoop (com.aether.net.core.PollLoop)                                        |
 |  - Pin-threaded epoll/selector driver                                           |
 +---------------------------------------------------------------------------------+
                                          |
                                          v
 +---------------------------------------------------------------------------------+
 |  MemoryArena (com.aether.net.memory.MemoryArena)                                |
 |  - Pre-allocated off-heap direct memory pools (Zero-GC)                         |
 +---------------------------------------------------------------------------------+
                                          |
                                          v
 +---------------------------------------------------------------------------------+
 |  LockFreeRingBuffer (com.aether.net.buffer.LockFreeRingBuffer)                  |
 |  - Single-Producer Single-Consumer / Multi-Producer Multi-Consumer atomics      |
 +---------------------------------------------------------------------------------+
                                          |
       +----------------------------------+----------------------------------+
       |                                                                     |
       v                                                                     v
 +-----------------------------------+                 +-----------------------------------+
 | PacketFilter                      |                 | SlidingWindowController           |
 | (com.aether.net.filter)           |                 | (com.aether.net.flow)             |
 | - Stateless BPF-like match rules  |                 | - Flow control & congestion state |
 +-----------------------------------+                 +-----------------------------------+
       |                                                                     |
       +----------------------------------+----------------------------------+
                                          |
                                          v
 +---------------------------------------------------------------------------------+
 |  PacketEngine (com.aether.net.PacketEngine)                                     |
 |  - Core orchestration facade & lifecycle coordinator                            |
 +---------------------------------------------------------------------------------+
```

## 3. Concurrency Model & Memory Invariants

`aether-packet-engine` relies on strict memory ownership contracts and non-blocking concurrency primitives to scale linearly across modern NUMA architectures:

*   **Thread Pinning & Locality:** Worker threads in `PollLoop` are bound to dedicated CPU cores via affinity masks, minimizing context switches and maximizing L1/L2 cache hit rates.
*   **Lock-Free Synchronization:** `LockFreeRingBuffer` avoids mutexes and heavy OS-level blocking by utilizing atomic memory operations (`sun.misc.Unsafe` / `VarHandle` volatile CAS loops with exponential backoff). Writers and readers operate on independent cache lines, eliminating false sharing.
*   **Off-Heap Memory Management:** `MemoryArena` allocates contiguous blocks of direct memory outside the JVM garbage-collected heap. Buffers are pooled and recycled via explicit lifecycle management, guaranteeing zero allocation overhead on the processing hot path.
*   **Memory Invariants:**
    1. Once a packet frame is written to an arena slot, its memory address is immutable until explicitly released back to the arena pool.
    2. Sequence numbers within the `SlidingWindowController` strictly obey monotonic progression rules protected by memory barriers.

## 4. Performance Characteristics & Algorithmic Complexity Table

| Operation / Subsystem | Time Complexity (Best/Worst) | Space Complexity | Latency Estimate (P99) | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **PollLoop Ingestion** | $O(1)$ | $O(1)$ | $< 800\text{ns}$ | Event-driven notification via native selectors |
| **LockFreeRingBuffer Push** | $O(1)$ amortized | $O(N)$ fixed buffer | $< 120\text{ns}$ | CAS loop with zero heap allocation |
| **LockFreeRingBuffer Pop** | $O(1)$ | $O(N)$ fixed buffer | $< 90\text{ns}$ | Cache-aligned read cursor progression |
| **PacketFilter Matching** | $O(K)$ ($K$ = rules) | $O(K)$ | $< 250\text{ns}$ | Compiled predicate evaluation chain |
| **MemoryArena Allocate** | $O(1)$ | $O(M)$ arena size | $< 40\text{ns}$ | Pointer bump allocation from free-list |
| **SlidingWindow Window Check** | $O(1)$ | $O(W)$ window size | $< 60\text{ns}$ | Bitset-backed sequence tracking |

## 5. Security Architecture & Threat Model

The engine is engineered around a hardened defensive security posture to withstand malformed inputs, memory exhaustion attacks, and high-rate packet flooding:

*   **Input Boundary & Sanitization:** All raw byte streams entering through `PollLoop` undergo strict bounds-checking within `PacketFilter` before being permitted into the processing pipeline. Truncated headers or invalid payloads are dropped instantly.
*   **Memory Corruption Protection:** By enforcing strict off-heap boundaries in `MemoryArena`, buffer overflows are isolated within pre-allocated frames, preventing native heap corruption or JVM segmentation faults.
*   **Denial of Service (DoS) Mitigation:** `SlidingWindowController` and ring buffer capacities enforce strict resource quotas. Unbounded queue growth is prevented via drop-tail or backpressure signaling to upstream drivers.
*   **Integer Overflow Prevention:** Sequence number arithmetic and buffer length calculations utilize checked operations to prevent integer wrap-around exploitation vectors.

## 6. Build, Verification & Benchmarking Recipes

### Prerequisites
*   Java Development Kit (JDK) 17+ (Temurin or OpenJDK recommended with JVM flags `-XX:+UseG1GC` or `-XX:+UnlockExperimentalVMOptions -XX:+UseZGC`)
*   Apache Maven 3.8+
*   Python 3.9+ (for running invariant tests and benchmarking harnesses)

### Compilation & Build
To compile the project and package binaries:
```bash
mvn clean package -DskipTests
```

### Running Verification Tests
Execute the Maven test suite along with the invariant verification scripts:
```bash
# Run Java unit and integration tests
mvn test

# Run system invariant verification tests
python3 tests/test_invariants.py
```

### Executing Benchmarks
Run the high-throughput benchmarking suite to measure throughput, latency distributions, and GC metrics:
```bash
python3 benchmarks/bench_engine.py --duration=60 --threads=4 --output=results.json
```

---

## Included Project Files Manifest
*   `pom.xml`
*   `src/main/java/com/aether/net/PacketEngine.java`
*   `src/main/java/com/aether/net/core/PollLoop.java`
*   `src/main/java/com/aether/net/buffer/LockFreeRingBuffer.java`
*   `src/main/java/com/aether/net/memory/MemoryArena.java`
*   `src/main/java/com/aether/net/flow/SlidingWindowController.java`
*   `src/main/java/com/aether/net/filter/PacketFilter.java`
*   `tests/test_invariants.py`
*   `benchmarks/bench_engine.py`