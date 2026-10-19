package com.aether.net.filter;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * High-performance, zero-allocation packet filter implementing an eBPF-like bytecode state machine.
 * Evaluates L3/L4 headers in O(1) time complexity using jump tables and direct memory inspection
 * via off-heap addresses, ensuring zero object instantiation on the hot path.
 */
public final class PacketFilter {

    // Opcode definitions for the bytecode instruction set
    public static final byte OP_LOAD8      = 0x01;
    public static final byte OP_LOAD16     = 0x02;
    public static final byte OP_LOAD32     = 0x03;
    public static final byte OP_JEQ        = 0x04;
    public static final byte OP_JNE        = 0x05;
    public static final byte OP_JGT        = 0x06;
    public static final byte OP_JGE        = 0x07;
    public static final byte OP_JLT        = 0x08;
    public static final byte OP_JLE        = 0x09;
    public static final byte OP_AND        = 0x0A;
    public static final byte OP_OR         = 0x0B;
    public static final byte OP_RET        = 0x0C;

    // Return flags
    public static final int RET_DROP       = 0x00;
    public static final int RET_PASS       = 0x01;

    private static final int MAX_INSTRUCTIONS = 1024;
    private static final int INSTRUCTION_SIZE = 16; // 1 byte op, 1 byte pad, 2 bytes reg/offset, 4 bytes imm, 8 bytes reserved/jump target

    // Packed instruction buffer layout:
    // [0]: Opcode (byte)
    // [1]: Register / Source (byte)
    // [2-3]: Offset/Field (short)
    // [4-7]: Immediate value (int)
    // [8-15]: Jump Target / Extended Data (long)
    private final byte[] bytecodeBuffer;
    private final int instructionCount;

    private static final sun.misc.Unsafe UNSAFE;

    static {
        try {
            java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Constructs a PacketFilter from a raw compiled bytecode byte array.
     * 
     * @param compiledBytecode the serialized filter program
     */
    public PacketFilter(byte[] compiledBytecode) {
        Objects.requireNonNull(compiledBytecode, "Compiled bytecode cannot be null");
        if (compiledBytecode.length % INSTRUCTION_SIZE != 0) {
            throw new IllegalArgumentException("Bytecode length must be a multiple of instruction size (" + INSTRUCTION_SIZE + ")");
        }
        this.instructionCount = compiledBytecode.length / INSTRUCTION_SIZE;
        if (this.instructionCount > MAX_INSTRUCTIONS) {
            throw new IllegalArgumentException("Instruction count exceeds maximum allowed: " + MAX_INSTRUCTIONS);
        }
        this.bytecodeBuffer = new byte[compiledBytecode.length];
        System.arraycopy(compiledBytecode, 0, this.bytecodeBuffer, 0, compiledBytecode.length);
    }

    /**
     * Evaluates the packet payload located at the given off-heap memory address against the compiled filter program.
     * 
     * @param packetAddress native memory address pointing to the packet payload
     * @param packetLength  total length of the packet payload in bytes
     * @return RET_PASS (1) if the packet matches, RET_DROP (0) otherwise
     */
    public int evaluate(long packetAddress, int packetLength) {
        if (packetAddress == 0L || packetLength <= 0) {
            return RET_DROP;
        }

        long pc = 0; // Program counter (instruction index)
        long accumulator = 0; // Working register for evaluation

        while (pc >= 0 && pc < instructionCount) {
            long baseOffset = pc * INSTRUCTION_SIZE;
            
            // Fetch instruction fields using direct array reads to avoid allocations
            int op = bytecodeBuffer[(int) baseOffset] & 0xFF;
            int reg = bytecodeBuffer[(int) baseOffset + 1] & 0xFF;
            short offset = (short) (((bytecodeBuffer[(int) baseOffset + 2] & 0xFF) << 8) | 
                                    (bytecodeBuffer[(int) baseOffset + 3] & 0xFF));
            int imm = ((bytecodeBuffer[(int) baseOffset + 4] & 0xFF) << 24) |
                      ((bytecodeBuffer[(int) baseOffset + 5] & 0xFF) << 16) |
                      ((bytecodeBuffer[(int) baseOffset + 6] & 0xFF) << 8)  |
                      (bytecodeBuffer[(int) baseOffset + 7] & 0xFF);
            
            long targetOrData = 0L;
            for (int i = 0; i < 8; i++) {
                targetOrData |= ((long) (bytecodeBuffer[(int) baseOffset + 8 + i] & 0xFF)) << (56 - (i * 8));
            }

            switch (op) {
                case OP_LOAD8: {
                    long memAddr = packetAddress + offset;
                    if (offset < 0 || offset + 1 > packetLength) {
                        return RET_DROP; // Out of bounds memory access protection
                    }
                    accumulator = UNSAFE.getByte(memAddr) & 0xFFL;
                    pc++;
                    break;
                }
                case OP_LOAD16: {
                    long memAddr = packetAddress + offset;
                    if (offset < 0 || offset + 2 > packetLength) {
                        return RET_DROP;
                    }
                    // Network byte order (Big Endian) conversion
                    short val = UNSAFE.getShort(memAddr);
                    accumulator = Short.reverseBytes(val) & 0xFFFFL;
                    pc++;
                    break;
                }
                case OP_LOAD32: {
                    long memAddr = packetAddress + offset;
                    if (offset < 0 || offset + 4 > packetLength) {
                        return RET_DROP;
                    }
                    // Network byte order (Big Endian) conversion
                    int val = UNSAFE.getInt(memAddr);
                    accumulator = Integer.reverseBytes(val) & 0xFFFFFFFFL;
                    pc++;
                    break;
                }
                case OP_JEQ: {
                    if (accumulator == imm) {
                        pc = targetOrData;
                    } else {
                        pc++;
                    }
                    break;
                }
                case OP_JNE: {
                    if (accumulator != imm) {
                        pc = targetOrData;
                    } else {
                        pc++;
                    }
                    break;
                }
                case OP_JGT: {
                    if (accumulator > imm) {
                        pc = targetOrData;
                    } else {
                        pc++;
                    }
                    break;
                }
                case OP_JGE: {
                    if (accumulator >= imm) {
                        pc = targetOrData;
                    } else {
                        pc++;
                    }
                    break;
                }
                case OP_JLT: {
                    if (accumulator < imm) {
                        pc = targetOrData;
                    } else {
                        pc++;
                    }
                    break;
                }
                case OP_JLE: {
                    if (accumulator <= imm) {
                        pc = targetOrData;
                    } else {
                        pc++;
                    }
                    break;
                }
                case OP_AND: {
                    accumulator &= imm;
                    pc++;
                    break;
                }
                case OP_OR: {
                    accumulator |= imm;
                    pc++;
                    break;
                }
                case OP_RET: {
                    return (int) (accumulator != 0 ? RET_PASS : RET_DROP);
                }
                default: {
                    // Unknown opcode encountered, fail-safe drop
                    return RET_DROP;
                }
            }
        }

        return RET_DROP;
    }

    /**
     * Builder class for programmatically constructing compiled bytecode packets for the PacketFilter.
     */
    public static final class Builder {
        private final byte[] tempBuffer = new byte[MAX_INSTRUCTIONS * INSTRUCTION_SIZE];
        private int currentOffset = 0;

        public Builder emitLoad8(int offset) {
            emit(OP_LOAD8, 0, (short) offset, 0, 0L);
            return this;
        }

        public Builder emitLoad16(int offset) {
            emit(OP_LOAD16, 0, (short) offset, 0, 0L);
            return this;
        }

        public Builder emitLoad32(int offset) {
            emit(OP_LOAD32, 0, (short) offset, 0, 0L);
            return this;
        }

        public Builder emitJeq(int imm, long targetPc) {
            emit(OP_JEQ, 0, (short) 0, imm, targetPc);
            return this;
        }

        public Builder emitJne(int imm, long targetPc) {
            emit(OP_JNE, 0, (short) 0, imm, targetPc);
            return this;
        }

        public Builder emitJgt(int imm, long targetPc) {
            emit(OP_JGT, 0, (short) 0, imm, targetPc);
            return this;
        }

        public Builder emitJge(int imm, long targetPc) {
            emit(OP_JGE, 0, (short) 0, imm, targetPc);
            return this;
        }

        public Builder emitJlt(int imm, long targetPc) {
            emit(OP_JLT, 0, (short) 0, imm, targetPc);
            return this;
        }

        public Builder emitJle(int imm, long targetPc) {
            emit(OP_JLE, 0, (short) 0, imm, targetPc);
            return this;
        }

        public Builder emitAnd(int imm) {
            emit(OP_AND, 0, (short) 0, imm, 0L);
            return this;
        }

        public Builder emitOr(int imm) {
            emit(OP_OR, 0, (short) 0, imm, 0L);
            return this;
        }

        public Builder emitRet() {
            emit(OP_RET, 0, (short) 0, 0, 0L);
            return this;
        }

        private void emit(byte op, int reg, short offset, int imm, long targetOrData) {
            if (currentOffset + INSTRUCTION_SIZE > tempBuffer.length) {
                throw new IllegalStateException("Maximum instruction limit reached");
            }
            tempBuffer[currentOffset++] = op;
            tempBuffer[currentOffset++] = (byte) reg;
            tempBuffer[currentOffset++] = (byte) (offset >> 8);
            tempBuffer[currentOffset++] = (byte) offset;
            
            tempBuffer[currentOffset++] = (byte) (imm >> 24);
            tempBuffer[currentOffset++] = (byte) (imm >> 16);
            tempBuffer[currentOffset++] = (byte) (imm >> 8);
            tempBuffer[currentOffset++] = (byte) imm;

            for (int i = 7; i >= 0; i--) {
                tempBuffer[currentOffset++] = (byte) (targetOrData >> (i * 8));
            }
        }

        public PacketFilter build() {
            byte[] finalCode = new byte[currentOffset];
            System.arraycopy(tempBuffer, 0, finalCode, 0, currentOffset);
            return new PacketFilter(finalCode);
        }
    }
}