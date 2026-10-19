package org.example.benchmark;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance dedicated throughput and latency microbenchmark for aether-packet-engine.
 * Measures operations per second (ops/sec) and p50/p99 latency percentiles across high iterations.
 * Outputs clear formatted tabular metrics to stdout.
 * 
 * Note: Designed to be self-contained with zero external dependencies beyond the JDK,
 * mocking the precise structural interfaces of aether-packet-engine for deterministic benchmarking.
 */
public class AetherPacketEngineBenchmark {

    // --- Mocking aether-packet-engine core interfaces for the benchmark ---
    
    public static class PacketBuffer {
        private final byte[] data;
        private int length;

        public PacketBuffer(int capacity) {
            this.data = new byte[capacity];
            this.length = 0;
        }

        public void write(byte[] src, int len) {
            System.arraycopy(src, 0, data, 0, len);
            this.length = len;
        }

        public int getLength() {
            return length;
        }

        public byte[] getData() {
            return data;
        }
    }

    public interface PacketPipeline {
        boolean process(PacketBuffer buffer);
    }

    public static class DefaultPacketPipeline implements PacketPipeline {
        @Override
        public boolean process(PacketBuffer buffer) {
            // Simulate deep packet inspection (DPI), header parsing, and routing lookup
            byte[] data = buffer.getData();
            int len = buffer.getLength();
            
            long checksum = 0;
            for (int i = 0; i < len; i++) {
                checksum += (data[i] & 0xFF);
            }
            
            // Artificial light compute load mimicking stateful packet inspection
            return (checksum % 2) == 0 || len > 0;
        }
    }

    // --- Benchmark Configuration ---
    
    private static final int WARMUP_ITERATIONS = 2_000_000;
    private static final int MEASUREMENT_ITERATIONS = 10_000_000;
    private static final int PACKET_SIZE = 512; // Typical Ethernet/IP payload slice

    public static void main(String[] args) {
        System.out.println("Initializing Aether Packet Engine Microbenchmark...");
        
        PacketPipeline pipeline = new DefaultPacketPipeline();
        byte[] payload = new byte[PACKET_SIZE];
        ThreadLocalRandom.current().nextBytes(payload);
        
        PacketBuffer buffer = new PacketBuffer(1500);
        buffer.write(payload, PACKET_SIZE);

        // --- WARMUP PHASE (JIT compilation & tier optimization) ---
        System.out.println("Running warmup (" + WARMUP_ITERATIONS + " iterations)...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            pipeline.process(buffer);
        }
        
        // Force GC prior to measurement
        System.gc();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // --- MEASUREMENT PHASE ---
        System.out.println("Running measurement (" + MEASUREMENT_ITERATIONS + " iterations)...");
        
        // Latency array tracking individual execution times in nanoseconds
        // Pre-allocated to avoid allocation overhead during critical path
        long[] latencies = new long[MEASUREMENT_ITERATIONS];
        
        AtomicLong successCounter = new AtomicLong(0);
        
        long startTime = System.nanoTime();
        
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long startNano = System.nanoTime();
            
            boolean result = pipeline.process(buffer);
            
            long endNano = System.nanoTime();
            
            latencies[i] = endNano - startNano;
            if (result) {
                successCounter.incrementAndGet();
            }
        }
        
        long endTime = System.nanoTime();
        long totalDurationNs = endTime - startTime;
        
        // --- METRICS CALCULATION ---
        
        double totalDurationSec = totalDurationNs / 1_000_000_000.0;
        double opsPerSec = MEASUREMENT_ITERATIONS / totalDurationSec;
        
        // Sort latencies for percentile calculations
        Arrays.sort(latencies);
        
        double p50 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.50)] / 1000.0; // convert to microseconds
        double p90 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.90)] / 1000.0;
        double p99 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.99)] / 1000.0;
        double p999 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.999)] / 1000.0;
        double max = latencies[MEASUREMENT_ITERATIONS - 1] / 1000.0;

        // --- FORMATTED TABULAR OUTPUT ---
        
        System.out.println("\n");
        System.out.println("==================================================================");
        System.out.println("         AETHER PACKET ENGINE - BENCHMARK RESULTS                 ");
        System.out.println("==================================================================");
        System.out.printf("| %-28s | %-31s |\n", "Metric", "Result");
        System.out.println("------------------------------------------------------------------");
        System.out.printf("| %-28s | %-31d |\n", "Total Iterations", MEASUREMENT_ITERATIONS);
        System.out.printf("| %-28s | %-31.2f |\n", "Total Duration (sec)", totalDurationSec);
        System.out.printf("| %-28s | %-31.2f |\n", "Throughput (ops/sec)", opsPerSec);
        System.out.printf("| %-28s | %-31.3f |\n", "Latency P50 (us)", p50);
        System.out.printf("| %-28s | %-31.3f |\n", "Latency P90 (us)", p90);
        System.out.printf("| %-28s | %-31.3f |\n", "Latency P99 (us)", p99);
        System.out.printf("| %-28s | %-31.3f |\n", "Latency P99.9 (us)", p999);
        System.out.printf("| %-28s | %-31.3f |\n", "Latency Max (us)", max);
        System.out.println("==================================================================");
    }
}