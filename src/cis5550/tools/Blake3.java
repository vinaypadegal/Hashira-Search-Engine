package cis5550.tools;

import java.nio.charset.StandardCharsets;

/**
 * Minimal unkeyed BLAKE3-256 implementation (pure Java, single-threaded).
 * Suitable as a drop-in replacement for Murmur3 when better diffusion and
 * prefix-independence are desired without adding external dependencies.
 *
 * This implementation focuses on the default hash (32-byte output) without
 * XOF or keyed/derive-key modes.
 */
public final class Blake3 {
  private static final int CHUNK_LEN = 1024;
  private static final int BLOCK_LEN = 64;
  private static final int OUT_LEN = 32;
  private static final int ROUNDS = 7;

  private static final int[] IV = {
      0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
      0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19
  };

  private static final int CHUNK_START = 1;
  private static final int CHUNK_END = 2;
  private static final int PARENT = 4;
  private static final int ROOT = 8;

  // Message permutation applied once per round.
  private static final int[] MSG_PERMUTATION = {
      2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8
  };

  private Blake3() {}

  public static byte[] digest(byte[] input) {
    byte[] data = input == null ? new byte[0] : input;
    if (data.length <= CHUNK_LEN) {
      return hashSingleChunk(data, 0, data.length, true);
    }

    int offset = 0;
    long chunkIndex = 0;
    int[][] cvs = new int[(data.length + CHUNK_LEN - 1) / CHUNK_LEN][];
    int cvCount = 0;
    while (offset < data.length) {
      int len = Math.min(CHUNK_LEN, data.length - offset);
      cvs[cvCount++] = hashChunkCv(data, offset, len, chunkIndex);
      offset += len;
      chunkIndex++;
    }
    return reduceParentsToDigest(cvs, cvCount);
  }

  public static String hashToString(String input) {
    byte[] out = digest(input == null ? new byte[0] : input.getBytes(StandardCharsets.UTF_8));
    StringBuilder sb = new StringBuilder(out.length * 2);
    for (byte b : out) {
      int idx = (b >= 0) ? b : 256 + b;
      sb.append(Hasher.byte2chars[idx]);
    }
    return sb.toString();
  }

  private static byte[] hashSingleChunk(byte[] data, int offset, int len, boolean root) {
    int blocks = (len + BLOCK_LEN - 1) / BLOCK_LEN;
    int[] cv = IV.clone();
    int[] state = null;
    for (int i = 0; i < blocks; i++) {
      int blockLen = Math.min(BLOCK_LEN, len - i * BLOCK_LEN);
      int flags = 0;
      if (i == 0) flags |= CHUNK_START;
      if (i == blocks - 1) flags |= CHUNK_END;
      if (root && i == blocks - 1) flags |= ROOT;

      int[] blockWords = loadBlockWords(data, offset + i * BLOCK_LEN, blockLen);
      state = compress(cv, blockWords, 0, blockLen, flags);
      cv = xorHalves(state);
    }
    if (state == null) {
      // Empty input case.
      state = compress(IV, new int[16], 0, 0, CHUNK_START | CHUNK_END | ROOT);
    }
    return outputBytes(state);
  }

  private static int[] hashChunkCv(byte[] data, int offset, int len, long chunkIndex) {
    int blocks = (len + BLOCK_LEN - 1) / BLOCK_LEN;
    int[] cv = IV.clone();
    for (int i = 0; i < blocks; i++) {
      int blockLen = Math.min(BLOCK_LEN, len - i * BLOCK_LEN);
      int flags = 0;
      if (i == 0) flags |= CHUNK_START;
      if (i == blocks - 1) flags |= CHUNK_END;

      int[] blockWords = loadBlockWords(data, offset + i * BLOCK_LEN, blockLen);
      int[] state = compress(cv, blockWords, chunkIndex, blockLen, flags);
      cv = xorHalves(state);
    }
    return cv;
  }

  private static byte[] reduceParentsToDigest(int[][] cvs, int cvCount) {
    int[][] current = cvs;
    int count = cvCount;
    while (count > 1) {
      if (count == 2) {
        int[] left = current[0];
        int[] right = current[1];
        int[] state = parentState(left, right, true);
        return outputBytes(state);
      }

      int nextCount = (count + 1) / 2;
      int[][] next = new int[nextCount][];
      int writeIdx = 0;
      int i = 0;
      while (i + 1 < count) {
        next[writeIdx++] = parentCv(current[i], current[i + 1]);
        i += 2;
      }
      if (i < count) {
        next[writeIdx++] = current[i];
      }
      current = next;
      count = writeIdx;
    }

    // Fallback; should not be hit for input > CHUNK_LEN.
    int[] single = current[0];
    int[] block = new int[16];
    System.arraycopy(single, 0, block, 0, 8);
    int[] state = compress(IV, block, 0, BLOCK_LEN, ROOT);
    return outputBytes(state);
  }

  private static int[] parentCv(int[] left, int[] right) {
    return xorHalves(parentState(left, right, false));
  }

  private static int[] parentState(int[] left, int[] right, boolean isRoot) {
    int[] blockWords = new int[16];
    System.arraycopy(left, 0, blockWords, 0, 8);
    System.arraycopy(right, 0, blockWords, 8, 8);
    int flags = PARENT | (isRoot ? ROOT : 0);
    return compress(IV, blockWords, 0, BLOCK_LEN, flags);
  }

  private static int[] compress(int[] cv, int[] blockWords, long counter, int blockLen, int flags) {
    int[] v = new int[16];
    System.arraycopy(cv, 0, v, 0, 8);
    System.arraycopy(IV, 0, v, 8, 4);
    v[12] = (int) counter;
    v[13] = (int) (counter >>> 32);
    v[14] = blockLen;
    v[15] = flags;

    int[] schedule = blockWords.clone();
    for (int round = 0; round < ROUNDS; round++) {
      // Column rounds.
      g(v, 0, 4, 8, 12, schedule[0], schedule[1]);
      g(v, 1, 5, 9, 13, schedule[2], schedule[3]);
      g(v, 2, 6, 10, 14, schedule[4], schedule[5]);
      g(v, 3, 7, 11, 15, schedule[6], schedule[7]);
      // Diagonal rounds.
      g(v, 0, 5, 10, 15, schedule[8], schedule[9]);
      g(v, 1, 6, 11, 12, schedule[10], schedule[11]);
      g(v, 2, 7, 8, 13, schedule[12], schedule[13]);
      g(v, 3, 4, 9, 14, schedule[14], schedule[15]);

      schedule = permute(schedule);
    }

    return v;
  }

  private static int[] xorHalves(int[] state) {
    int[] cv = new int[8];
    for (int i = 0; i < 8; i++) {
      cv[i] = state[i] ^ state[i + 8];
    }
    return cv;
  }

  private static int[] loadBlockWords(byte[] data, int offset, int blockLen) {
    int[] words = new int[16];
    int end = offset + blockLen;
    for (int i = 0; i < 16; i++) {
      int word = 0;
      int base = offset + (i * 4);
      for (int j = 0; j < 4; j++) {
        int idx = base + j;
        if (idx < end) {
          word |= (data[idx] & 0xFF) << (8 * j);
        }
      }
      words[i] = word;
    }
    return words;
  }

  private static byte[] outputBytes(int[] state) {
    byte[] out = new byte[OUT_LEN];
    int written = 0;
    for (int word : state) {
      for (int j = 0; j < 4 && written < OUT_LEN; j++) {
        out[written++] = (byte) ((word >>> (8 * j)) & 0xFF);
      }
      if (written >= OUT_LEN) break;
    }
    return out;
  }

  private static int[] permute(int[] words) {
    int[] next = new int[16];
    for (int i = 0; i < 16; i++) {
      next[i] = words[MSG_PERMUTATION[i]];
    }
    return next;
  }

  private static void g(int[] v, int a, int b, int c, int d, int x, int y) {
    v[a] = v[a] + v[b] + x;
    v[d] = rotr(v[d] ^ v[a], 16);
    v[c] = v[c] + v[d];
    v[b] = rotr(v[b] ^ v[c], 12);
    v[a] = v[a] + v[b] + y;
    v[d] = rotr(v[d] ^ v[a], 8);
    v[c] = v[c] + v[d];
    v[b] = rotr(v[b] ^ v[c], 7);
  }

  private static int rotr(int x, int n) {
    return (x >>> n) | (x << (32 - n));
  }
}

