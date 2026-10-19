package com.aether.net.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * High-performance, edge-triggered Linux epoll integration poll loop utilizing 
 * Project Panama foreign linker APIs to multiplex network descriptors with zero context-switch waste.
 * Designed for the aether-packet-engine, operating entirely off-heap with strict thread affinity.
 */
public final class PollLoop implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(PollLoop.class.getName());

    // Native epoll constants (Linux x86_64 / AArch64)
    private static final int EPOLL_CLOEXEC = 02000000;
    private static final int EPOLL_NONBLOCK = 00004000;
    private static final int EPOLL_CTL_ADD = 1;
    private static final int EPOLL_CTL_MOD = 3;
    private static final int EPOLL_CTL_DEL = 2;
    private static final int EPOLLIN = 0x001;
    private static final int EPOLLOUT = 0x004;
    private static final int EPOLLERR = 0x008;
    private static final int EPOLLHUP = 0x010;
    private static final int EPOLLET = 1 << 31;

    private final int epollFd;
    private final Arena nativeArena;
    private final MemorySegment eventsSegment;
    private final int maxEvents;
    private final AtomicBoolean running;
    private final Thread pollerThread;
    private final PollHandler handler;

    private static final VarHandle STATE_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE_HANDLE = l.findVarHandle(PollLoop.class, "shutdownState", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused")
    private volatile int shutdownState = 0; // 0: Active, 1: Shutting Down, 2: Terminated

    @FunctionalInterface
    public interface PollHandler {
        void handleEvent(int fd, int events);
        void onError(int fd, Throwable cause);
    }

    public PollLoop(int maxEvents, PollHandler handler) throws IOException {
        this.maxEvents = maxEvents;
        this.handler = handler;
        this.running = new AtomicBoolean(false);
        this.nativeArena = Arena.ofShared();

        // Create epoll instance via native call
        int fd = epollCreate1(EPOLL_CLOEXEC);
        if (fd < 0) {
            throw new IOException("Failed to create epoll instance. Errno: " + (-fd));
        }
        this.epollFd = fd;

        // Allocate native memory for epoll_event array:
        // struct epoll_event { uint32_t events; epoll_data_t data; };
        // On 64-bit Linux, sizeof(struct epoll_event) = 12 bytes (packed) or 16 bytes (aligned due to 8-byte pointer data).
        // We use 16 bytes per event to safely accommodate alignment.
        long eventStructSize = 16;
        this.eventsSegment = nativeArena.allocate(eventStructSize * maxEvents, 8);

        this.pollerThread = Thread.ofPlatform()
                .name("aether-poll-loop-" + epollFd)
                .unstarted(this::runLoop);
    }

    public void register(SelectableChannel channel, int fd, int events) throws IOException {
        int nativeEvents = translateEvents(events) | EPOLLET;
        int res = epollCtl(epollFd, EPOLL_CTL_ADD, fd, nativeEvents, fd);
        if (res < 0) {
            throw new IOException("Failed to add file descriptor " + fd + " to epoll. Errno: " + (-res));
        }
    }

    public void modify(int fd, int events) throws IOException {
        int nativeEvents = translateEvents(events) | EPOLLET;
        int res = epollCtl(epollFd, EPOLL_CTL_MOD, fd, nativeEvents, fd);
        if (res < 0) {
            throw new IOException("Failed to modify file descriptor " + fd + " in epoll. Errno: " + (-res));
        }
    }

    public void unregister(int fd) throws IOException {
        int res = epollCtl(epollFd, EPOLL_CTL_DEL, fd, 0, fd);
        if (res < 0) {
            // Log warning but do not throw to prevent teardown blockages
            LOGGER.log(Level.WARNING, "Failed to remove file descriptor {0} from epoll. Errno: {1}", 
                    new Object[]{fd, -res});
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            pollerThread.start();
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            STATE_HANDLE.setVolatile(this, 1);
            pollerThread.interrupt();
            try {
                pollerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Interrupted while waiting for PollLoop thread termination.", e);
            }
            STATE_HANDLE.setVolatile(this, 2);
        }
    }

    private void runLoop() {
        MemorySegment eventsPtr = this.eventsSegment;
        int efd = this.epollFd;
        
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                int n = epollWait(efd, eventsPtr, maxEvents, -1);
                if (n < 0) {
                    int errno = -n;
                    if (errno == 4 /* EINTR */) {
                        continue;
                    }
                    if (!running.get()) {
                        break;
                    }
                    LOGGER.log(Level.SEVERE, "epoll_wait failure. Errno: {0}", errno);
                    break;
                }

                for (int i = 0; i < n; i++) {
                    long structOffset = i * 16L;
                    int events = eventsPtr.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, structOffset);
                    int fd = eventsPtr.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, structOffset + 8);

                    try {
                        handler.handleEvent(fd, translateNativeEvents(events));
                    } catch (Throwable t) {
                        handler.onError(fd, t);
                    }
                }
            } catch (Throwable t) {
                if (!running.get()) {
                    break;
                }
                LOGGER.log(Level.SEVERE, "Unexpected exception in PollLoop execution path.", t);
            }
        }
    }

    private int translateEvents(int events) {
        int nativeEvents = 0;
        if ((events & 0x01) != 0) nativeEvents |= EPOLLIN;
        if ((events & 0x04) != 0) nativeEvents |= EPOLLOUT;
        return nativeEvents;
    }

    private int translateNativeEvents(int nativeEvents) {
        int events = 0;
        if ((nativeEvents & EPOLLIN) != 0) events |= 0x01;
        if ((nativeEvents & EPOLLOUT) != 0) events |= 0x04;
        if ((nativeEvents & (EPOLLERR | EPOLLHUP)) != 0) events |= 0x08;
        return events;
    }

    @Override
    public void close() {
        stop();
        try {
            closeNativeFd(epollFd);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error closing epoll file descriptor.", e);
        } finally {
            if (nativeArena.scope().isAlive()) {
                nativeArena.close();
            }
        }
    }

    // Native Method Declarations linking to system calls via JNI/Panama
    private static native int epollCreate1(int flags);
    private static native int epollCtl(int epfd, int op, int fd, int events, long dataFd);
    private static native int epollWait(int epfd, MemorySegment events, int maxevents, int timeout);
    private static native int closeNativeFd(int fd);

    static {
        // Load native library bindings for system operations
        System.loadLibrary("aether_packet_engine_native");
    }
}