package cis5550.tools;

/**
 * MurmurHash3 - Fast, non-cryptographic hash function optimized for distributed systems
 * Based on the MurmurHash3 algorithm (used by Cassandra, Elasticsearch, etc.)
 * 
 * This provides better performance than SHA-1 for non-cryptographic use cases
 * while maintaining excellent distribution properties.
 */
public class MurmurHash3 {
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int R3 = 33;
    private static final int M = 5;
    private static final int N1 = 0x52dce729;
    private static final int N2 = 0x38495ab5;

    /**
     * Hash a string using MurmurHash3 (128-bit variant, returns lower 64 bits)
     * This is faster than SHA-1 and provides excellent distribution for distributed systems
     */
    public static long hash64(String input) {
        if (input == null) return 0;
        return hash64(input.getBytes());
    }

    /**
     * Hash bytes using MurmurHash3
     */
    public static long hash64(byte[] data) {
        return hash64(data, 0, data.length, 0);
    }

    /**
     * Hash bytes with seed
     */
    public static long hash64(byte[] data, int offset, int length, long seed) {
        long h1 = seed;
        long h2 = seed;

        int roundedEnd = offset + (length & 0xFFFFFFF0);  // round down to 16 byte block
        for (int i = offset; i < roundedEnd; i += 16) {
            long k1 = getLongLittleEndian(data, i);
            long k2 = getLongLittleEndian(data, i + 8);

            k1 *= C1;
            k1 = Long.rotateLeft(k1, R1);
            k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, R2);
            h1 += h2;
            h1 = h1 * M + N1;

            k2 *= C2;
            k2 = Long.rotateLeft(k2, R3);
            k2 *= C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, R1);
            h2 += h1;
            h2 = h2 * M + N2;
        }

        long k1 = 0;
        long k2 = 0;

        int rem = length & 0xF;
        if (rem > 0) {
            switch (rem) {
                case 15: k2 ^= ((long) data[roundedEnd + 14]) << 48;
                case 14: k2 ^= ((long) data[roundedEnd + 13]) << 40;
                case 13: k2 ^= ((long) data[roundedEnd + 12]) << 32;
                case 12: k2 ^= ((long) data[roundedEnd + 11]) << 24;
                case 11: k2 ^= ((long) data[roundedEnd + 10]) << 16;
                case 10: k2 ^= ((long) data[roundedEnd + 9]) << 8;
                case 9:  k2 ^= ((long) data[roundedEnd + 8]);
                         k2 *= C2;
                         k2 = Long.rotateLeft(k2, R3);
                         k2 *= C1;
                         h2 ^= k2;
                case 8:  k1 ^= ((long) data[roundedEnd + 7]) << 56;
                case 7:  k1 ^= ((long) data[roundedEnd + 6]) << 48;
                case 6:  k1 ^= ((long) data[roundedEnd + 5]) << 40;
                case 5:  k1 ^= ((long) data[roundedEnd + 4]) << 32;
                case 4:  k1 ^= ((long) data[roundedEnd + 3]) << 24;
                case 3:  k1 ^= ((long) data[roundedEnd + 2]) << 16;
                case 2:  k1 ^= ((long) data[roundedEnd + 1]) << 8;
                case 1:  k1 ^= ((long) data[roundedEnd]);
                         k1 *= C1;
                         k1 = Long.rotateLeft(k1, R1);
                         k1 *= C2;
                         h1 ^= k1;
            }
        }

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        return h1;
    }

    private static long getLongLittleEndian(byte[] data, int offset) {
        return ((long) data[offset] & 0xff) |
               (((long) data[offset + 1] & 0xff) << 8) |
               (((long) data[offset + 2] & 0xff) << 16) |
               (((long) data[offset + 3] & 0xff) << 24) |
               (((long) data[offset + 4] & 0xff) << 32) |
               (((long) data[offset + 5] & 0xff) << 40) |
               (((long) data[offset + 6] & 0xff) << 48) |
               (((long) data[offset + 7] & 0xff) << 56);
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    /**
     * Convert hash to string format compatible with existing system
     * Uses same byte2chars mapping as Hasher for compatibility
     */
    public static String hashToString(String input) {
        long hash = hash64(input);
        return longToHashString(hash);
    }

    private static String longToHashString(long value) {
        // Use same byte2chars mapping as Hasher for compatibility
        String[] byte2chars = Hasher.byte2chars;
        StringBuilder result = new StringBuilder();
        
        // Convert long to bytes and map using byte2chars
        for (int i = 0; i < 8; i++) {
            byte b = (byte) ((value >>> (i * 8)) & 0xFF);
            int index = (b > 0) ? b : 255 + b;
            result.append(byte2chars[index]);
        }
        
        return result.toString();
    }
}

