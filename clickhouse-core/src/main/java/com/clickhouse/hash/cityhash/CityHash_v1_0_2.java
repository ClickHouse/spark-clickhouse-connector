/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.hash.cityhash;


// modified from https://github.com/dpoluyanov/achord/blob/master/src/main/java/com/github/mangelion/achord/CityHash_v1_0_2.java
// 1. fixed some bugs involving int32 to uint32 conversion
// 2. changed netty ByteBuf to raw byte[]
final public class CityHash_v1_0_2 {

    private static final long kMul = 0x9ddfea08eb382d69L;
    // Some primes between 2^63 and 2^64 for various uses.
    private static final long k0 = 0xc3a5c85c97cb3127L;
    private static final long k1 = 0xb492b66fbe98f273L;
    private static final long k2 = 0x9ae16a3b2f90404fL;
    private static final long k3 = 0xc949d7c7509e6557L;

    private CityHash_v1_0_2() { /* restricted */ }

    private static long toLongLE(byte[] b, int i) {
        return 0xffffffffffffffffL &
                (((long) b[i + 7] << 56) +
                        ((long) (b[i + 6] & 255) << 48) +
                        ((long) (b[i + 5] & 255) << 40) +
                        ((long) (b[i + 4] & 255) << 32) +
                        ((long) (b[i + 3] & 255) << 24) +
                        ((b[i + 2] & 255) << 16) +
                        ((b[i + 1] & 255) << 8) +
                        ((b[i + 0] & 255) << 0));
    }

    private static long toIntLE(byte[] b, int i) {
        return 0xffffffffL &
                (((b[i + 3] & 255) << 24) +
                        ((b[i + 2] & 255) << 16) +
                        ((b[i + 1] & 255) << 8) +
                        ((b[i + 0] & 255) << 0));
    }

    private static long Fetch64(byte[] p, int index) {
        return toLongLE(p, index);
    }

    private static long Fetch32(byte[] p, int index) {
        return toIntLE(p, index);
    }

    // Equivalent to Rotate(), but requires the second arg to be non-zero.
    // On x86-64, and probably others, it's possible for this to compile
    // to a single instruction if both args are already in registers.
    private static long RotateByAtLeast1(long val, int shift) {
        return (val >>> shift) | (val << (64 - shift));
    }

    private static long ShiftMix(long val) {
        return val ^ (val >>> 47);
    }

    private static long Uint128Low64(UInt128 x) {
        return x.first;
    }

    private static long Rotate(long val, int shift) {
        return shift == 0 ? val : (val >>> shift) | (val << (64 - shift));
    }

    private static long Uint128High64(UInt128 x) {
        return x.second;
    }

    // Hash 128 input bits down to 64 bits of output.
    // This is intended to be a reasonably good hash function.
    public static long Hash128to64(UInt128 x) {
        // Murmur-inspired hashing.
        long a = (Uint128Low64(x) ^ Uint128High64(x)) * kMul;
        a ^= (a >>> 47);
        long b = (Uint128High64(x) ^ a) * kMul;
        b ^= (b >>> 47);
        b *= kMul;
        return b;
    }

    private static long HashLen16(long u, long v) {
        return Hash128to64(UInt128.of(u, v));
    }

    private static long HashLen0to16(byte[] s, int index, int len) {
        if (len > 8) {
            long a = Fetch64(s, index);
            long b = Fetch64(s, index + len - 8);
            return HashLen16(a, RotateByAtLeast1(b + len, len)) ^ b;
        }
        if (len >= 4) {
            long a = Fetch32(s, index);
            return HashLen16(len + (a << 3), Fetch32(s, index + len - 4));
        }
        if (len > 0) {
            byte a = s[index];
            byte b = s[index + len >>> 1];
            byte c = s[index + len - 1];
            int y = (a & 0xFF) + ((b & 0xFF) << 8);
            int z = len + ((c & 0xFF) << 2);
            return ShiftMix(y * k2 ^ z * k3) * k2;
        }
        return k2;
    }

    // This probably works well for 16-byte strings as well, but it may be overkill
    // in that case.
    private static long HashLen17to32(byte[] s, int index, int len) {
        long a = Fetch64(s, index) * k1;
        long b = Fetch64(s, index + 8);
        long c = Fetch64(s, index + len - 8) * k2;
        long d = Fetch64(s, index + len - 16) * k0;
        return HashLen16(Rotate(a - b, 43) + Rotate(c, 30) + d,
                a + Rotate(b ^ k3, 20) - c + len);
    }

    // Return a 16-byte hash for 48 bytes.  Quick and dirty.
    // Callers do best to use "random-looking" values for a and b.
    private static UInt128 WeakHashLen32WithSeeds(
            long w, long x, long y, long z, long a, long b) {
        a += w;
        b = Rotate(b + a + z, 21);
        long c = a;
        a += x;
        a += y;
        b += Rotate(a, 44);
        return UInt128.of(a + z, b + c);
    }

    // Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
    private static UInt128 WeakHashLen32WithSeeds(byte[] s, int index, long a, long b) {
        return WeakHashLen32WithSeeds(Fetch64(s, index),
                Fetch64(s, index + 8),
                Fetch64(s, index + 16),
                Fetch64(s, index + 24),
                a,
                b);
    }

    // Return an 8-byte hash for 33 to 64 bytes.
    private static long HashLen33to64(byte[] s, int index, int len) {
        long z = Fetch64(s, index + 24);
        long a = Fetch64(s, index) + (len + Fetch64(s, index + len - 16)) * k0;
        long b = Rotate(a + z, 52);
        long c = Rotate(a, 37);
        a += Fetch64(s, index + 8);
        c += Rotate(a, 7);
        a += Fetch64(s, index + 16);
        long vf = a + z;
        long vs = b + Rotate(a, 31) + c;
        a = Fetch64(s, index + 16) + Fetch64(s, index + len - 32);
        z = Fetch64(s, index + len - 8);
        b = Rotate(a + z, 52);
        c = Rotate(a, 37);
        a += Fetch64(s, index + len - 24);
        c += Rotate(a, 7);
        a += Fetch64(s, index + len - 16);
        long wf = a + z;
        long ws = b + Rotate(a, 31) + c;
        long r = ShiftMix((vf + ws) * k2 + (wf + vs) * k0);
        return ShiftMix(r * k0 + vs) * k2;
    }

    // A subroutine for CityHash128().  Returns a decent 128-bit hash for strings
    // of any length representable in size_t.  Based on City and Murmur.
    private static UInt128 CityMurmur(byte[] s, int index, int len, UInt128 seed) {
        long a = Uint128Low64(seed);
        long b = Uint128High64(seed);
        long c;
        long d;
        int l = len - 16;
        if (l <= 0) {  // len <= 16
            a = ShiftMix(a * k1) * k1;
            c = b * k1 + HashLen0to16(s, index, len);
            d = ShiftMix(a + (len >= 8 ? Fetch64(s, index) : c));
        } else {  // len > 16
            c = HashLen16(Fetch64(s, index + len - 8) + k1, a);
            d = HashLen16(b + len, c + Fetch64(s, index + len - 16));
            a += d;
            do {
                a ^= ShiftMix(Fetch64(s, index) * k1) * k1;
                a *= k1;
                b ^= a;
                c ^= ShiftMix(Fetch64(s, index + 8) * k1) * k1;
                c *= k1;
                d ^= c;
                index += 16;
                l -= 16;
            } while (l > 0);
        }
        a = HashLen16(a, c);
        b = HashLen16(d, b);
        return UInt128.of(a ^ b, HashLen16(b, a));
    }

    public static long CityHash64(byte[] s, int index, int len) {
        if (len <= 32) {
            if (len <= 16) {
                return HashLen0to16(s, index, len);
            } else {
                return HashLen17to32(s, index, len);
            }
        } else if (len <= 64) {
            return HashLen33to64(s, index, len);
        }

        // For strings over 64 bytes we hash the end first, and then as we
        // loop we keep 56 bytes of state: v, w, x, y, and z.
        long x = Fetch64(s, index);
        long y = Fetch64(s, index + len - 16) ^ k1;
        long z = Fetch64(s, index + len - 56) ^ k0;
        UInt128 v = WeakHashLen32WithSeeds(s, len - 64, len, y);
        UInt128 w = WeakHashLen32WithSeeds(s, len - 32, len * k1, k0);
        z += ShiftMix(v.second) * k1;
        x = Rotate(z + x, 39) * k1;
        y = Rotate(y, 33) * k1;

        // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
        len = (len - 1) & ~63;
        do {
            x = Rotate(x + y + v.first + Fetch64(s, index + 16), 37) * k1;
            y = Rotate(y + v.second + Fetch64(s, index + 48), 42) * k1;
            x ^= w.second;
            y ^= v.first;
            z = Rotate(z ^ w.first, 33);
            v = WeakHashLen32WithSeeds(s, index, v.second * k1, x + w.first);
            w = WeakHashLen32WithSeeds(s, index + 32, z + w.second, y);
            // swap
            long t = z;
            z = x;
            x = t;
            index += 64;
            len -= 64;
        } while (len != 0);
        return HashLen16(HashLen16(v.first, w.first) + ShiftMix(y) * k1 + z,
                HashLen16(v.second, w.second) + x);
    }

    private static long CityHash64WithSeed(byte[] s, int index, int len, long seed) {
        return CityHash64WithSeeds(s, index, len, k2, seed);
    }

    private static long CityHash64WithSeeds(byte[] s, int index, int len,
                                            long seed0, long seed1) {
        return HashLen16(CityHash64(s, index, len) - seed0, seed1);
    }

    private static UInt128 CityHash128WithSeed(byte[] s, int index, int len, UInt128 seed) {
        if (len < 128) {
            return CityMurmur(s, index, len, seed);
        }

        // We expect len >= 128 to be the common case.  Keep 56 bytes of state:
        // v, w, x, y, and z.
        UInt128 v, w;
        long x = Uint128Low64(seed);
        long y = Uint128High64(seed);
        long z = len * k1;
        long vFirst = Rotate(y ^ k1, 49) * k1 + Fetch64(s, index);
        long vSecond = Rotate(vFirst, 42) * k1 + Fetch64(s, index + 8);
        long wFirst = Rotate(y + z, 35) * k1 + x;
        long wSecond = Rotate(x + Fetch64(s, index + 88), 53) * k1;

        // This is the same inner loop as CityHash64(), manually unrolled.
        do {
            x = Rotate(x + y + vFirst + Fetch64(s, index + 16), 37) * k1;
            y = Rotate(y + vSecond + Fetch64(s, index + 48), 42) * k1;
            x ^= wSecond;
            y ^= vFirst;
            z = Rotate(z ^ wFirst, 33);
            v = WeakHashLen32WithSeeds(s, index, vSecond * k1, x + wFirst);
            w = WeakHashLen32WithSeeds(s, index + 32, z + wSecond, y);

            vFirst = v.first;
            vSecond = v.second;
            wFirst = w.first;
            wSecond = w.second;
            {
                long swap = z;
                z = x;
                x = swap;
            }
            index += 64;
            x = Rotate(x + y + vFirst + Fetch64(s, index + 16), 37) * k1;
            y = Rotate(y + vSecond + Fetch64(s, index + 48), 42) * k1;
            x ^= wSecond;
            y ^= vFirst;
            z = Rotate(z ^ wFirst, 33);
            v = WeakHashLen32WithSeeds(s, index, vSecond * k1, x + wFirst);
            w = WeakHashLen32WithSeeds(s, index + 32, z + wSecond, y);

            vFirst = v.first;
            vSecond = v.second;
            wFirst = w.first;
            wSecond = w.second;
            {
                long swap = z;
                z = x;
                x = swap;
            }
            index += 64;
            len -= 128;
        } while (len >= 128);
        y += Rotate(wFirst, 37) * k0 + z;
        x += Rotate(vFirst + z, 49) * k0;
        // If 0 < len < 128, hash up to 4 chunks of 32 bytes each from the end of s.
        for (int tail_done = 0; tail_done < len; ) {
            tail_done += 32;
            y = Rotate(y - x, 42) * k0 + vSecond;
            wFirst += Fetch64(s, index + len - tail_done + 16);
            x = Rotate(x, 49) * k0 + wFirst;
            wFirst += vFirst;
            v = WeakHashLen32WithSeeds(s, index + len - tail_done, vFirst, vSecond);

            vFirst = v.first;
            vSecond = v.second;
        }
        // At this point our 48 bytes of state should contain more than
        // enough information for a strong 128-bit hash.  We use two
        // different 48-byte-to-8-byte hashes to get a 16-byte final result.
        x = HashLen16(x, vFirst);
        y = HashLen16(y, wFirst);
        return UInt128.of(HashLen16(x + vSecond, wSecond) + y,
                HashLen16(x + wSecond, y + vSecond));
    }

    public static UInt128 CityHash128(byte[] s, int len) {
        if (len >= 16) {
            return CityHash128WithSeed(s, 16,
                    len - 16,
                    UInt128.of(Fetch64(s, 0) ^ k3,
                            Fetch64(s, 8)));
        } else if (len >= 8) {
            return CityHash128WithSeed(null,
                    0, 0,
                    UInt128.of(Fetch64(s, 0) ^ (len * k0),
                            Fetch64(s, len - 8) ^ k1));
        } else {
            return CityHash128WithSeed(s, 0, len, UInt128.of(k0, k1));
        }
    }
}