package com.aether.net.memory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import sun.misc.Unsafe;

public final class MemoryArena implements AutoCloseable {

    private static final Unsafe UNSAFE;
    static {
        try {
            java.lang.reflect.Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize sun.misc.Unsafe", e);
        }
    }

    private static final VarHandle REF_COUNT_HANDLE;
    static {
        try {
            REF_COUNT_HANDLE = MethodHandles.lookup().findVarHandle(
                MemoryArena.class, "referenceCount", long.class
            );
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize VarHandle for referenceCount", e);
        }
    }

    private final long baseAddress;
    private final long sizeBytes;
    private final Arena foreignArena;
    private final MemorySegment segment;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    @SuppressWarnings("unused")
    private volatile long referenceCount = 1L;

    private final long[] freeListHeads;
    private final long[] chunkSizes;
    private final int minChunkShift;
    private final int maxOrder;

    public MemoryArena(long sizeBytes, int minChunkShift, int maxOrder) {
        if (sizeBytes <= 0) {
            throw new IllegalArgumentException("Arena size must be greater than zero.");
        }
        if (minChunkShift < 4) {
            throw new IllegalArgumentException("Minimum chunk shift must be at least 4 (16 bytes).");
        }
        if (maxOrder <= 0) {
            throw new IllegalArgumentException("Max order must be greater than zero.");
        }

        this.sizeBytes = sizeBytes;
        this.minChunkShift = minChunkShift;
        this.maxOrder = maxOrder;
        
        this.foreignArena = Arena.ofShared();
        this.segment = foreignArena.allocate(sizeBytes, 64);
        this.baseAddress = segment.address();

        int numOrders = maxOrder + 1;
        this.freeListHeads = new long[numOrders];
        this.chunkSizes = new long[numOrders];

        for (int i = 0; i <= maxOrder; i++) {
            freeListHeads[i] = 0L;
            chunkSizes[i] = 1L << (minChunkShift + i);
        }

        long maxChunkSize = chunkSizes[maxOrder];
        if (maxChunkSize > sizeBytes) {
            throw new IllegalArgumentException("Max chunk size exceeds total arena size.");
        }

        long currentBlockAddr = baseAddress;
        long remainingSize = sizeBytes;

        while (remainingSize >= maxChunkSize) {
            setNextPointer(currentBlockAddr, freeListHeads[maxOrder]);
            freeListHeads[maxOrder] = currentBlockAddr;
            currentBlockAddr += maxChunkSize;
            remainingSize -= maxChunkSize;
        }

        for (int order = maxOrder - 1; order >= 0; order--) {
            long chunkSize = chunkSizes[order];
            while (remainingSize >= chunkSize) {
                setNextPointer(currentBlockAddr, freeListHeads[order]);
                freeListHeads[order] = currentBlockAddr;
                currentBlockAddr += chunkSize;
                remainingSize -= chunkSize;
            }
        }
    }

    public long allocate(long requestedBytes) {
        if (requestedBytes <= 0) {
            throw new IllegalArgumentException("Requested bytes must be positive.");
        }

        acquireReference();
        try {
            if (isClosed.get()) {
                throw new IllegalStateException("MemoryArena is closed.");
            }

            int targetOrder = findOrderForSize(requestedBytes);
            if (targetOrder > maxOrder) {
                releaseReference();
                throw new IllegalArgumentException("Requested allocation exceeds maximum chunk size.");
            }

            for (int currentOrder = targetOrder; currentOrder <= maxOrder; currentOrder++) {
                long chunkAddr = removeChunkFromFreeList(currentOrder);
                if (chunkAddr != 0L) {
                    while (currentOrder > targetOrder) {
                        currentOrder--;
                        long buddyAddr = chunkAddr + chunkSizes[currentOrder];
                        addChunkToFreeList(buddyAddr, currentOrder);
                    }
                    setAllocationMetadata(chunkAddr, targetOrder);
                    return chunkAddr + 16L;
                }
            }

            releaseReference();
            throw new OutOfMemoryError("MemoryArena exhausted: unable to satisfy allocation request.");
        } catch (Throwable t) {
            releaseReference();
            throw t;
        }
    }

    public void free(long payloadAddress) {
        if (payloadAddress == 0L) {
            return;
        }

        long chunkAddr = payloadAddress - 16L;
        if (chunkAddr < baseAddress || chunkAddr >= baseAddress + sizeBytes) {
            throw new IllegalArgumentException("Address is outside the bounds of this MemoryArena.");
        }

        int order = getAllocationMetadata(chunkAddr);
        if (order < 0 || order > maxOrder) {
            throw new IllegalStateException("Corrupted allocation metadata at address: " + chunkAddr);
        }

        long currentChunkAddr = chunkAddr;
        int currentOrder = order;

        while (currentOrder < maxOrder) {
            long chunkSize = chunkSizes[currentOrder];
            long relativeOffset = currentChunkAddr - baseAddress;
            long buddyOffset = relativeOffset ^ chunkSize;
            long buddyAddr = baseAddress + buddyOffset;

            if (buddyAddr < baseAddress || buddyAddr >= baseAddress + sizeBytes) {
                break;
            }

            if (!removeSpecificChunkFromFreeList(buddyAddr, currentOrder)) {
                break;
            }

            if (buddyAddr < currentChunkAddr) {
                currentChunkAddr = buddyAddr;
            }
            currentOrder++;
        }

        addChunkToFreeList(currentChunkAddr, currentOrder);
        releaseReference();
    }

    private int findOrderForSize(long bytes) {
        long effectiveBytes = bytes + 16L;
        for (int i = 0; i <= maxOrder; i++) {
            if (chunkSizes[i] >= effectiveBytes) {
                return i;
            }
        }
        return maxOrder + 1;
    }

    private long removeChunkFromFreeList(int order) {
        long head = freeListHeads[order];
        if (head == 0L) {
            return 0L;
        }
        long next = getNextPointer(head);
        freeListHeads[order] = next;
        setNextPointer(head, 0L);
        return head;
    }

    private void addChunkToFreeList(long chunkAddr, int order) {
        setNextPointer(chunkAddr, freeListHeads[order]);
        freeListHeads[order] = chunkAddr;
    }

    private boolean removeSpecificChunkFromFreeList(long targetAddr, int order) {
        long head = freeListHeads[order];
        if (head == 0L) {
            return false;
        }

        if (head == targetAddr) {
            freeListHeads[order] = getNextPointer(head);
            setNextPointer(head, 0L);
            return true;
        }

        long current = head;
        long next = getNextPointer(current);

        while (next != 0L) {
            if (next == targetAddr) {
                long nextOfNext = getNextPointer(next);
                setNextPointer(current, nextOfNext);
                setNextPointer(next, 0L);
                return true;
            }
            current = next;
            next = getNextPointer(current);
        }

        return false;
    }

    private void setNextPointer(long chunkAddr, long nextAddr) {
        UNSAFE.putLong(chunkAddr, nextAddr);
    }

    private long getNextPointer(long chunkAddr) {
        return UNSAFE.getLong(chunkAddr);
    }

    private void setAllocationMetadata(long chunkAddr, int order) {
        UNSAFE.putLong(chunkAddr + 8L, (long) order);
    }

    private int getAllocationMetadata(long chunkAddr) {
        return (int) UNSAFE.getLong(chunkAddr + 8L);
    }

    private void acquireReference() {
        while (true) {
            long current = (long) REF_COUNT_HANDLE.getVolatile(this);
            if (current <= 0) {
                throw new IllegalStateException("MemoryArena is already destroyed or being destroyed.");
            }
            if (REF_COUNT_HANDLE.compareAndSet(this, current, current + 1)) {
                return;
            }
        }
    }

    private void releaseReference() {
        while (true) {
            long current = (long) REF_COUNT_HANDLE.getVolatile(this);
            if (current <= 0) {
                throw new IllegalStateException("Illegal reference count state.");
            }
            long next = current - 1;
            if (REF_COUNT_HANDLE.compareAndSet(this, current, next)) {
                if (next == 0 && isClosed.get()) {
                    doClose();
                }
                return;
            }
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            while (true) {
                long current = (long) REF_COUNT_HANDLE.getVolatile(this);
                if (current == 1) {
                    if (REF_COUNT_HANDLE.compareAndSet(this, 1, 0)) {
                        doClose();
                        return;
                    }
                } else {
                    Thread.onSpinWait();
                    if (current <= 0) {
                        return;
                    }
                }
            }
        }
    }

    private void doClose() {
        try {
            foreignArena.close();
        } catch (Throwable t) {
            // Suppress or log depending on framework logging; throwing in close is discouraged
        }
    }

    public long getBaseAddress() {
        return baseAddress;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public boolean isClosed() {
        return isClosed.get();
    }
}