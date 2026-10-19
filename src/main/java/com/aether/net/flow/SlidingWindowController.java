package com.aether.net.flow;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public final class SlidingWindowController {

    private static final long INITIAL_CWND = 10;
    private static final long MIN_CWND = 2;
    private static final long MAX_CWND = 65535;
    
    private static final double ALPHA = 0.125;
    private static final double BETA = 0.25;
    
    private static final long INITIAL_RTO_NANOS = 1_000_000_000L; // 1 second
    private static final long MIN_RTO_NANOS = 10_000_000L;       // 10 milliseconds
    private static final long MAX_RTO_NANOS = 30_000_000_000L;   // 30 seconds

    private volatile long cwnd;
    private volatile long ssthresh;
    
    private volatile double srtt;     // Smoothed RTT in nanoseconds
    private volatile double rttvar;   // RTT variance in nanoseconds
    private volatile long rtoNanos;   // Retransmission timeout in nanoseconds

    private final LongAdder packetsSent = new LongAdder();
    private final LongAdder packetsAcknowledged = new LongAdder();
    private final LongAdder packetsDropped = new LongAdder();
    private final LongAdder retransmissions = new LongAdder();

    private volatile long lastCongestionEventNanos = 0L;
    private volatile boolean dropTailMode = false;

    private static final VarHandle CWND_HANDLE;
    private static final VarHandle SSTHRESH_HANDLE;
    private static final VarHandle RTO_HANDLE;
    private static final VarHandle DROP_TAIL_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            CWND_HANDLE = l.findVarHandle(SlidingWindowController.class, "cwnd", long.class);
            SSTHRESH_HANDLE = l.findVarHandle(SlidingWindowController.class, "ssthresh", long.class);
            RTO_HANDLE = l.findVarHandle(SlidingWindowController.class, "rtoNanos", long.class);
            DROP_TAIL_HANDLE = l.findVarHandle(SlidingWindowController.class, "dropTailMode", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public SlidingWindowController(long initialSsthresh) {
        this.cwnd = INITIAL_CWND;
        this.ssthresh = initialSsthresh > 0 ? initialSsthresh : 32768L;
        this.srtt = -1.0;
        this.rttvar = -1.0;
        this.rtoNanos = INITIAL_RTO_NANOS;
        this.dropTailMode = false;
    }

    public boolean canSend(long inFlightBytes, double ringBufferSaturation) {
        if (ringBufferSaturation > 0.85) {
            DROP_TAIL_HANDLE.setVolatile(this, true);
            packetsDropped.increment();
            return false;
        } else if (ringBufferSaturation < 0.60) {
            DROP_TAIL_HANDLE.setVolatile(this, false);
        }

        if (dropTailMode) {
            packetsDropped.increment();
            return false;
        }

        long currentCwnd = (long) CWND_HANDLE.getVolatile(this);
        return inFlightBytes < currentCwnd;
    }

    public void onPacketSent() {
        packetsSent.increment();
    }

    public void onAckReceived(long rttSampleNanos, double ringBufferSaturation) {
        Objects.requireNonNull(rttSampleNanos, "rttSampleNanos cannot be null");
        if (rttSampleNanos < 0L) {
            throw new IllegalArgumentException("RTT sample cannot be negative: " + rttSampleNanos);
        }

        packetsAcknowledged.increment();
        updateRttEstimates(rttSampleNanos);
        performCongestionAvoidanceStep(ringBufferSaturation);
    }

    public void onPacketLossDetected(long currentTimestampNanos) {
        retransmissions.increment();
        lastCongestionEventNanos = currentTimestampNanos;

        long currentCwnd = (long) CWND_HANDLE.getVolatile(this);
        long newSsthresh = Math.max(currentCwnd / 2, MIN_CWND);
        SSTHRESH_HANDLE.setVolatile(this, newSsthresh);

        long newCwnd = Math.max(newSsthresh, MIN_CWND);
        CWND_HANDLE.setVolatile(this, newCwnd);

        // Exponential backoff of RTO
        long currentRto = (long) RTO_HANDLE.getVolatile(this);
        long doubledRto = Math.min(currentRto * 2L, MAX_RTO_NANOS);
        RTO_HANDLE.setVolatile(this, doubledRto);
    }

    private void updateRttEstimates(long rttSampleNanos) {
        if (srtt < 0.0) {
            // Initialize estimators per RFC 6298
            srtt = (double) rttSampleNanos;
            rttvar = (double) rttSampleNanos / 2.0;
        } else {
            double rttSampleD = (double) rttSampleNanos;
            rttvar = (1.0 - BETA) * rttvar + BETA * Math.abs(srtt - rttSampleD);
            srtt = (1.0 - ALPHA) * srtt + ALPHA * rttSampleD;
        }

        long computedRto = (long) (srtt + Math.max(1.0, 4.0 * rttvar));
        if (computedRto < MIN_RTO_NANOS) {
            computedRto = MIN_RTO_NANOS;
        } else if (computedRto > MAX_RTO_NANOS) {
            computedRto = MAX_RTO_NANOS;
        }
        RTO_HANDLE.setVolatile(this, computedRto);
    }

    private void performCongestionAvoidanceStep(double ringBufferSaturation) {
        long currentCwnd = (long) CWND_HANDLE.getVolatile(this);
        long currentSsthresh = (long) SSTHRESH_HANDLE.getVolatile(this);

        // Adaptive pacing based on memory ring buffer pressure
        if (ringBufferSaturation > 0.75) {
            return; // Hold congestion window steady under high load
        }

        if (currentCwnd < currentSsthresh) {
            // Slow Start phase: exponential increase
            long nextCwnd = currentCwnd * 2L;
            if (nextCwnd > MAX_CWND) {
                nextCwnd = MAX_CWND;
            }
            CWND_HANDLE.setVolatile(this, nextCwnd);
        } else {
            // Congestion Avoidance phase: linear increase (approx 1 packet per RTT)
            long nextCwnd = currentCwnd + 1L;
            if (nextCwnd > MAX_CWND) {
                nextCwnd = MAX_CWND;
            }
            CWND_HANDLE.setVolatile(this, nextCwnd);
        }
    }

    public long getCwnd() {
        return (long) CWND_HANDLE.getVolatile(this);
    }

    public long getSsthresh() {
        return (long) SSTHRESH_HANDLE.getVolatile(this);
    }

    public long getRtoNanos() {
        return (long) RTO_HANDLE.getVolatile(this);
    }

    public double getSmoothedRtt() {
        return srtt;
    }

    public double getRttVar() {
        return rttvar;
    }

    public boolean isDropTailMode() {
        return (boolean) DROP_TAIL_HANDLE.getVolatile(this);
    }

    public long getPacketsSent() {
        return packetsSent.sum();
    }

    public long getPacketsAcknowledged() {
        return packetsAcknowledged.sum();
    }

    public long getPacketsDropped() {
        return packetsDropped.sum();
    }

    public long getRetransmissions() {
        return retransmissions.sum();
    }

    public void reset() {
        CWND_HANDLE.setVolatile(this, INITIAL_CWND);
        SSTHRESH_HANDLE.setVolatile(this, 32768L);
        RTO_HANDLE.setVolatile(this, INITIAL_RTO_NANOS);
        srtt = -1.0;
        rttvar = -1.0;
        lastCongestionEventNanos = 0L;
        DROP_TAIL_HANDLE.setVolatile(this, false);
    }
}