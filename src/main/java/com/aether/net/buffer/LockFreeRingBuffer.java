package com.aether.net.buffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * High-performance, lock-free Multi-Producer Multi-Consumer (MPMC) circular ring buffer 
 * tailored for off-heap memory addresses within the 'aether-packet-engine'.
 * 
 * <p>Implements cache-line padding (64-byte alignment) to prevent false sharing across 
 * distinct CPU cores handling ingress, filtering, and packet processing. Uses sequence 
 * tracking with atomics and acquire-release semantics via {@link VarHandle}.
 */
public final class LockFreeRingBuffer {

    private static final long CACHE_LINE_SIZE = 64;
    private static final int INT_FIELD_SIZE = 4;
    
    // VarHandle for atomic sequence updates
    private static final VarHandle SEQUENCE_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            SEQUENCE_HANDLE = l.findVarHandle(SequenceNode.class, "sequence", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final int capacity;
    private final int mask;
    private final SequenceNode[] buffer;

    // Cache-line padding headers to prevent false sharing between head and tail pointers
    // P1 padding fields
    private long p01, p02, p03, p04, p05, p06, p07;
    
    // Tail pointer (producer write index)
    private volatile long tailSequence = 0L;
    
    // P2 padding fields
    private long p11, p12, p13, p14, p15, p16, p17;
    
    // Head pointer (consumer read index)
    private volatile long headSequence = 0L;
    
    // P3 padding fields
    private long p21, p22, p23, p24, p25, p26, p27;

    /**
     * Node structure containing a payload slot (memory address) and its associated sequence number.
     * Padded to eliminate cache line contention between adjacent array elements.
     */
    private static final class SequenceNode {
        // 64-byte padding before fields
        @SuppressWarnings("unused")
        private long pad0, pad1, pad2, pad3, pad4, pad5, pad6;

        private volatile long sequence;
        private long payload;

        @SuppressWarnings("unused")
        private long pad7, pad8, pad9, pada, padb, padc, padd;

        SequenceNode(long initialSequence) {
            this.sequence = initialSequence;
            this.payload = 0L;
        }
    }

    /**
     * Constructs a lock-free ring buffer with a capacity rounded up to the nearest power of two.
     * 
     * @desiredCapacity The requested capacity; will be adjusted to the next power of 2 if necessary.
     */
    public LockFreeRingBuffer(int desiredCapacity) {
        if (desiredCapacity <= 0) {
            throw new IllegalArgumentException("Capacity must be strictly positive: " + desiredCapacity);
        }
        
        // Round up to nearest power of two
        int pow2Capacity = 1;
        while (pow2Capacity < desiredCapacity) {
            pow2Capacity <<= 1;
        }
        
        this.capacity = pow2Capacity;
        this.mask = pow2Capacity - 1;
        this.buffer = new SequenceNode[pow2Capacity];
        
        for (int i = 0; i < pow2Capacity; i++) {
            buffer[i] = new SequenceNode(i);
        }
    }

    /**
     * Offers a native memory address (payload) to the ring buffer in a lock-free, multi-producer safe manner.
     * 
     * @param memoryAddress The off-heap memory address pointing to a packet payload.
     * @return {@code true} if the element was successfully appended; {@code false} if the buffer is full.
     */
    public boolean offer(long memoryAddress) {
        SequenceNode node;
        long tail = tailSequence;
        
        for (;;) {
            int index = (int) (tail & mask);
            node = buffer[index];
            long seq = node.sequence;
            long dif = seq - tail;

            if (dif == 0L) {
                // The node is available. Try to advance the tail sequence via CAS.
                if (UNSAFE_CAS_TAIL(tail, tail + 1L)) {
                    break;
                }
            } else if (dif < 0L) {
                // The buffer is fully saturated.
                return false;
            } else {
                // Another thread advanced the tail; reload.
                tail = tailSequence;
            }
        }

        node.payload = memoryAddress;
        // Publish the node by setting its sequence number (release semantics).
        SEQUENCE_HANDLE.setRelease(node, tail + 1L);
        return true;
    }

    /**
     * Polls a native memory address from the ring buffer in a lock-free, multi-consumer safe manner.
     * 
     * @return The native memory address of the packet payload, or {@code 0L} if the buffer is empty.
     */
    public long poll() {
        SequenceNode node;
        long head = headSequence;
        
        for (;;) {
            int index = (int) (head & mask);
            node = buffer[index];
            long seq = node.sequence;
            long dif = seq - (head + 1L);

            if (dif == 0L) {
                // Node is ready to be consumed. Try to advance the head sequence via CAS.
                if (UNSAFE_CAS_HEAD(head, head + 1L)) {
                    break;
                }
            } else if (dif < 0L) {
                // The buffer is empty.
                return 0L;
            } else {
                // Another thread advanced the head; reload.
                head = headSequence;
            }
        }

        long payload = node.payload;
        node.payload = 0L;
        // Reset the sequence number for the next wrap-around cycle (release semantics).
        SEQUENCE_HANDLE.setRelease(node, head + mask + 1L);
        return payload;
    }

    /**
     * Evaluates the current number of elements present in the ring buffer.
     * 
     * <p>Note: Due to concurrent modifications, this value is an instantaneous snapshot 
     * and should only be used for telemetry and flow control metrics.
     * 
     * @return Approximate count of elements in the ring buffer.
     */
    public int size() {
        long currentHead = headSequence;
        long currentTail = tailSequence;
        long size = currentTail - currentHead;
        if (size < 0L) {
            return 0;
        }
        return (size > capacity) ? capacity : (int) size;
    }

    /**
     * Checks if the ring buffer contains zero elements.
     * 
     * @return {@code true} if empty, {@code false} otherwise.
     */
    public boolean isEmpty() {
        return headSequence == tailSequence;
    }

    /**
     * Retrieves the total fixed capacity of the ring buffer.
     * 
     * @return The power-of-two buffer capacity.
     */
    public int capacity() {
        return this.capacity;
    }

    /**
     * Compares and swaps the tail sequence pointer.
     */
    private boolean UNSAFE_CAS_TAIL(long expected, long update) {
        // VarHandle update for tailSequence using acquire/release semantics
        return TAIL_HANDLE.compareAndSet(this, expected, update);
    }

    /**
     * Compares and swaps the head sequence pointer.
     */
    private boolean UNSAFE_CAS_HEAD(long expected, long update) {
        // VarHandle update for headSequence using acquire/release semantics
        return HEAD_HANDLE.compareAndSet(this, expected, update);
    }

    // VarHandles for head and tail pointer synchronization
    private static final VarHandle TAIL_HANDLE;
    private static final VarHandle HEAD_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            TAIL_HANDLE = l.findVarHandle(LockFreeRingBuffer.class, "tailSequence", long.class);
            HEAD_HANDLE = l.findVarHandle(LockFreeRingBuffer.class, "headSequence", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}