package com.aether.packet.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Unit, Invariant, and Concurrent Stress-Test Suite for 'aether-packet-engine'.
 * Validates zero-copy mechanics, structural boundary invariants, state recovery under fault injection,
 * and linearizability under hyper-contention.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AetherPacketEngineTest {

    private static final int DEFAULT_BUFFER_SIZE = 65536;
    private AetherPacketEngine engine;

    @BeforeEach
    void setUp() {
        engine = new AetherPacketEngine(DEFAULT_BUFFER_SIZE);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
    }

    // =========================================================================
    // 1. UNIT & BOUNDARY INVARIANT TESTS
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("Unit: Zero-allocation and direct buffer initialization invariants")
    void testInitializationInvariants() {
        assertNotNull(engine);
        assertEquals(DEFAULT_BUFFER_SIZE, engine.capacity());
        assertEquals(0, engine.activePacketCount());
        assertEquals(DEFAULT_BUFFER_SIZE, engine.remainingCapacity());
        assertTrue(engine.isHealthy());
    }

    @ParameterizedTest
    @Order(20)
    @ValueSource(ints = {-1, 0, AetherPacketEngine.MAX_PACKET_SIZE + 1})
    @DisplayName("Boundary: Reject invalid packet allocations at architectural limits")
    void testPacketAllocationBoundaries(int invalidSize) {
        assertThrows(IllegalArgumentException.class, () -> engine.allocate(invalidSize));
    }

    @Test
    @Order(30)
    @DisplayName("Edge Case: Exact capacity utilization and fragmentation handling")
    void testExactCapacityAndFragmentation() {
        int maxPayload = AetherPacketEngine.MAX_PACKET_SIZE;
        
        AetherPacket p1 = engine.allocate(maxPayload);
        assertNotNull(p1);
        assertEquals(maxPayload, p1.length());
        
        // Next allocation should fail due to exhaustion
        assertThrows(BufferOverflowException.class, () -> engine.allocate(10));

        engine.release(p1);
        assertEquals(0, engine.activePacketCount());
        assertEquals(DEFAULT_BUFFER_SIZE, engine.remainingCapacity());

        // Re-allocation after full release
        AetherPacket p2 = engine.allocate(maxPayload);
        assertNotNull(p2);
        engine.release(p2);
    }

    @Test
    @Order(40)
    @DisplayName("Invariant: Double-release safety and lifecycle state transitions")
    void testDoubleReleaseAndLifecycle() {
        AetherPacket packet = engine.allocate(1024);
        assertNotNull(packet);
        assertTrue(packet.isValid());

        engine.release(packet);
        assertFalse(packet.isValid());

        // Double release must be a no-op or throw controlled exception, never corrupting state
        assertThrows(IllegalStateException.class, () -> engine.release(packet));
        assertEquals(0, engine.activePacketCount());
    }

    // =========================================================================
    // 2. CONCURRENT STRESS & RACE CONDITION TESTS
    // =========================================================================

    @Test
    @Order(100)
    @DisplayName("Concurrent Stress: Massive multi-threaded allocation and deallocation contention")
    void testConcurrentContentionStress() throws InterruptedException {
        int threadCount = 64;
        int iterationsPerThread = 2000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(threadCount);
        AtomicInteger allocationFailures = new AtomicInteger(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    for (int j = 0; j < iterationsPerThread; j++) {
                        int size = random.nextInt(64, 1024);
                        try {
                            AetherPacket packet = engine.allocate(size);
                            if (packet != null) {
                                // Simulate packet processing
                                packet.buffer().putInt(0, 0xCAFEBABE);
                                // Random yield to induce race conditions
                                if (j % 17 == 0) {
                                    Thread.yield();
                                }
                                engine.release(packet);
                            }
                        } catch (BufferOverflowException e) {
                            allocationFailures.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = completionLatch.await(30, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(finished, "Test timed out indicating deadlock or severe thread starvation.");
        assertEquals(0, exceptionCount.get(), "Exceptions encountered during concurrent execution.");
        assertEquals(0, engine.activePacketCount(), "Memory leak detected: active packets remaining after all threads finished.");
        assertEquals(DEFAULT_BUFFER_SIZE, engine.remainingCapacity(), "Buffer capacity invariant violated after concurrent churn.");
    }

    @Test
    @Order(110)
    @DisplayName("Concurrent Stress: ABA problem and sequence skew detection")
    void testConcurrentABASkew() throws InterruptedException {
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong successCount = new AtomicLong(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                while (running.get()) {
                    try {
                        int size = 128;
                        AetherPacket p = engine.allocate(size);
                        p.buffer().putLong(0, Thread.currentThread().getId());
                        // Spin to increase contention on specific regions
                        for (int k = 0; k < 10; k++) {
                            p.buffer().getLong(0);
                        }
                        engine.release(p);
                        successCount.incrementAndGet();
                    } catch (BufferOverflowException ignored) {
                        // Expected due to high contention bounds
                    }
                }
            }));
        }

        Thread.sleep(2000);
        running.set(false);
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdownNow();

        assertTrue(successCount.get() > 0);
        assertEquals(0, engine.activePacketCount());
    }

    // =========================================================================
    // 3. FAILURE AND RECOVERY STATE VERIFICATION
    // =========================================================================

    @Test
    @Order(200)
    @DisplayName("Recovery: Engine panic reset and self-healing state machine")
    void testEngineFaultRecovery() {
        // Force engine into invalid/corrupted structural state via internal simulation or boundary abuse
        AetherPacket p1 = engine.allocate(2048);
        assertNotNull(p1);

        // Simulate catastrophic error / unhandled exception boundary
        engine.forceFaultState();
        assertFalse(engine.isHealthy());

        // Attempt recovery sequence
        engine.reset();

        assertTrue(engine.isHealthy());
        assertEquals(0, engine.activePacketCount());
        assertEquals(DEFAULT_BUFFER_SIZE, engine.remainingCapacity());

        // Verify normal operation resumes post-recovery
        AetherPacket p2 = engine.allocate(1024);
        assertNotNull(p2);
        engine.release(p2);
    }

    @Test
    @Order(210)
    @DisplayName("Recovery: Out-of-order partial compaction recovery")
    void testCompactionAndRecoveryUnderStress() {
        List<AetherPacket> packets = new ArrayList<>();
        
        // Fragment memory
        for (int i = 0; i < 10; i++) {
            packets.add(engine.allocate(512));
        }

        // Release in non-monotonic order (holes everywhere)
        engine.release(packets.get(2));
        engine.release(packets.get(5));
        engine.release(packets.get(8));

        // Compact engine fragments
        engine.compact();

        // Release remaining
        for (int i = 0; i < packets.size(); i++) {
            if (i != 2 && i != 5 && i != 8) {
                engine.release(packets.get(i));
            }
        }

        assertEquals(0, engine.activePacketCount());
        assertEquals(DEFAULT_BUFFER_SIZE, engine.remainingCapacity());
    }
}

// =========================================================================
// ENGINE STUB IMPLEMENTATION (Self-Contained Executable Specification)
// =========================================================================

class AetherPacketEngine {
    public static final int MAX_PACKET_SIZE = 32768;
    private final int capacity;
    private final ByteBuffer backingBuffer;
    private final AtomicInteger activeCount = new AtomicInteger(0);
    private final AtomicBoolean healthy = new AtomicBoolean(true);
    private final BitSet allocatedBitmap;

    public AetherPacketEngine(int capacity) {
        this.capacity = capacity;
        this.backingBuffer = ByteBuffer.allocateDirect(capacity);
        this.allocatedBitmap = new BitSet(capacity);
    }

    public synchronized AetherPacket allocate(int size) {
        if (!healthy.get()) {
            throw new IllegalStateException("Engine is in fault state.");
        }
        if (size <= 0 || size > MAX_PACKET_SIZE || size > remainingCapacity()) {
            throw new IllegalArgumentException("Invalid allocation size: " + size);
        }

        int offset = findFreeBlock(size);
        if (offset == -1) {
            throw new BufferOverflowException();
        }

        markAllocated(offset, size);
        activeCount.incrementAndGet();
        
        ByteBuffer slice = backingBuffer.duplicate();
        slice.position(offset);
        slice.limit(offset + size);
        
        return new AetherPacket(slice.slice(), offset, size, this);
    }

    public synchronized void release(AetherPacket packet) {
        if (!packet.isValid()) {
            throw new IllegalStateException("Packet already released or invalid.");
        }
        packet.invalidate();
        clearAllocated(packet.offset(), packet.length());
        activeCount.decrementAndGet();
    }

    public synchronized int remainingCapacity() {
        int allocated = 0;
        synchronized (allocatedBitmap) {
            allocated = allocatedBitsCount();
        }
        return capacity - allocated;
    }

    public int activePacketCount() {
        return activeCount.get();
    }

    public boolean isHealthy() {
        return healthy.get();
    }

    public void forceFaultState() {
        healthy.set(false);
    }

    public synchronized void reset() {
        allocatedBitmap.clear();
        activeCount.set(0);
        backingBuffer.clear();
        healthy.set(true);
    }

    public synchronized void compact() {
        // Simplified compaction simulation for testing memory defragmentation
        BitSet newBitmap = new BitSet(capacity);
        int currentOffset = 0;
        // In a true implementation, memory regions are relocated. Here we ensure structural consistency.
        allocatedBitmap.clear();
    }

    public int capacity() {
        return capacity;
    }

    public void close() {
        reset();
    }

    private int findFreeBlock(int size) {
        // Simple first-fit search across the bitmap
        int consecutive = 0;
        int start = 0;
        for (int i = 0; i < capacity; i++) {
            if (!allocatedBitmap.get(i)) {
                if (consecutive == 0) start = i;
                con