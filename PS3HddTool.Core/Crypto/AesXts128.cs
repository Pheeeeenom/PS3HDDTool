using System.Buffers.Binary;
using System.Runtime.Intrinsics;
using System.Security.Cryptography;

namespace PS3HddTool.Core.Crypto;

/// <summary>
/// AES-XTS-128 implementation for PS3 HDD decryption.
/// 
/// XTS mode:
///   1. Encrypt the sector number (as LE 16 bytes) with the tweak key → T
///   2. For each 16-byte block i in the sector:
///      a. PP = C[i] XOR T
///      b. CC = AES-ECB-Decrypt(PP) with data key
///      c. P[i] = CC XOR T
///      d. T = T * alpha (GF(2^128) multiply)
/// </summary>
public sealed class AesXts128 : IDisposable
{
    private readonly byte[] _dataKey;
    private readonly byte[] _tweakKey;

    public const int SectorSize = 512;
    public const int BlockSize = 16;
    private const int BlocksPerSector = SectorSize / BlockSize; // 32

    public AesXts128(byte[] dataKey, byte[] tweakKey)
    {
        if (dataKey.Length != 16)
            throw new ArgumentException("Data key must be 16 bytes.", nameof(dataKey));
        if (tweakKey.Length != 16)
            throw new ArgumentException("Tweak key must be 16 bytes.", nameof(tweakKey));

        _dataKey = (byte[])dataKey.Clone();
        _tweakKey = (byte[])tweakKey.Clone();
    }

    private sealed class ThreadCtx : IDisposable
    {
        public readonly Aes DataAes;
        public readonly Aes TweakAes;

        public ThreadCtx(byte[] dataKey, byte[] tweakKey)
        {
            DataAes = Aes.Create();
            DataAes.Key = dataKey;
            DataAes.Mode = CipherMode.ECB;
            DataAes.Padding = PaddingMode.None;

            TweakAes = Aes.Create();
            TweakAes.Key = tweakKey;
            TweakAes.Mode = CipherMode.ECB;
            TweakAes.Padding = PaddingMode.None;
        }

        public void Dispose()
        {
            DataAes.Dispose();
            TweakAes.Dispose();
        }
    }

    public byte[] EncryptSectors(byte[] plaintext, long startSectorNumber)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] ciphertext = new byte[plaintext.Length];
        ProcessAll(plaintext, ciphertext, startSectorNumber, encrypt: true);
        return ciphertext;
    }

    public byte[] DecryptSectors(byte[] ciphertext, long startSectorNumber)
    {
        if (ciphertext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] plaintext = new byte[ciphertext.Length];
        ProcessAll(ciphertext, plaintext, startSectorNumber, encrypt: false);
        return plaintext;
    }

    public void EncryptSectorsInto(byte[] plaintext, byte[] ciphertext, long startSectorNumber)
        => EncryptSectorsInto(plaintext, (Span<byte>)ciphertext, startSectorNumber);

    public void DecryptSectorsInto(byte[] ciphertext, byte[] plaintext, long startSectorNumber)
        => DecryptSectorsInto(ciphertext, (Span<byte>)plaintext, startSectorNumber);

    public void EncryptSectorsInto(byte[] plaintext, Span<byte> ciphertext, long startSectorNumber)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");
        if (ciphertext.Length < plaintext.Length)
            throw new ArgumentException("Output buffer too small.", nameof(ciphertext));

        ProcessAll(plaintext, ciphertext.Slice(0, plaintext.Length), startSectorNumber, encrypt: true);
    }

    public void DecryptSectorsInto(byte[] ciphertext, Span<byte> plaintext, long startSectorNumber)
    {
        if (ciphertext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");
        if (plaintext.Length < ciphertext.Length)
            throw new ArgumentException("Output buffer too small.", nameof(plaintext));

        ProcessAll(ciphertext, plaintext.Slice(0, ciphertext.Length), startSectorNumber, encrypt: false);
    }

    private unsafe void ProcessAll(byte[] input, Span<byte> output, long startSector, bool encrypt)
    {
        int sectorCount = input.Length / SectorSize;

        if (sectorCount < 64)
        {
            using var ctx = new ThreadCtx(_dataKey, _tweakKey);
            ProcessSectorRange(ctx, input, output, startSector, 0, sectorCount, encrypt);
            return;
        }

        int threadCount = Math.Min(Environment.ProcessorCount, sectorCount / 32);
        if (threadCount < 2) threadCount = 2;
        int sectorsPerThread = sectorCount / threadCount;

        fixed (byte* outPtr = output)
        {
            IntPtr captured = (IntPtr)outPtr;
            int outLen = output.Length;
            Parallel.For(0, threadCount,
                () => new ThreadCtx(_dataKey, _tweakKey),
                (thread, _, ctx) =>
                {
                    int startS = thread * sectorsPerThread;
                    int endS = (thread == threadCount - 1) ? sectorCount : startS + sectorsPerThread;
                    Span<byte> outSpan = new Span<byte>((void*)captured, outLen);
                    ProcessSectorRange(ctx, input, outSpan, startSector, startS, endS, encrypt);
                    return ctx;
                },
                ctx => ctx.Dispose());
        }
    }

    private static void ProcessSectorRange(ThreadCtx ctx, byte[] input, Span<byte> output,
        long startSector, int firstSector, int endSector, bool encrypt)
    {
        Span<byte> tweaks = stackalloc byte[SectorSize];
        Span<byte> work = stackalloc byte[SectorSize];
        Span<byte> tweakPlain = stackalloc byte[BlockSize];

        for (int s = firstSector; s < endSector; s++)
        {
            long sectorNumber = startSector + s;
            ReadOnlySpan<byte> srcSector = input.AsSpan(s * SectorSize, SectorSize);
            Span<byte> dstSector = output.Slice(s * SectorSize, SectorSize);

            // T_0 = aes-ecb(tweakKey, LE(sector_num))
            tweakPlain.Clear();
            BinaryPrimitives.WriteInt64LittleEndian(tweakPlain, sectorNumber);
            ctx.TweakAes.EncryptEcb(tweakPlain, tweaks.Slice(0, BlockSize), PaddingMode.None);

            // T_j = T_{j-1} * alpha for j = 1..31
            for (int j = 1; j < BlocksPerSector; j++)
            {
                Span<byte> prev = tweaks.Slice((j - 1) * BlockSize, BlockSize);
                Span<byte> cur = tweaks.Slice(j * BlockSize, BlockSize);
                prev.CopyTo(cur);
                GfMulAlpha(cur);
            }

            // work = src xor tweaks
            XorBlocks(srcSector, tweaks, work);

            // dst = aes-ecb(dataKey, work)
            if (encrypt)
                ctx.DataAes.EncryptEcb(work, dstSector, PaddingMode.None);
            else
                ctx.DataAes.DecryptEcb(work, dstSector, PaddingMode.None);

            // dst ^= tweaks
            XorBlocksInPlace(dstSector, tweaks);
        }
    }

    private static void XorBlocks(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, Span<byte> dst)
    {
        int i = 0;
        if (Vector128.IsHardwareAccelerated)
        {
            int simdEnd = a.Length - (a.Length % 16);
            for (; i < simdEnd; i += 16)
            {
                var va = Vector128.Create<byte>(a.Slice(i, 16));
                var vb = Vector128.Create<byte>(b.Slice(i, 16));
                (va ^ vb).CopyTo(dst.Slice(i, 16));
            }
        }
        for (; i < a.Length; i++) dst[i] = (byte)(a[i] ^ b[i]);
    }

    private static void XorBlocksInPlace(Span<byte> dst, ReadOnlySpan<byte> b)
    {
        int i = 0;
        if (Vector128.IsHardwareAccelerated)
        {
            int simdEnd = dst.Length - (dst.Length % 16);
            for (; i < simdEnd; i += 16)
            {
                var vd = Vector128.Create<byte>(dst.Slice(i, 16));
                var vb = Vector128.Create<byte>(b.Slice(i, 16));
                (vd ^ vb).CopyTo(dst.Slice(i, 16));
            }
        }
        for (; i < dst.Length; i++) dst[i] ^= b[i];
    }

    /// <summary>
    /// GF(2^128) multiply by alpha (x). 
    /// Left-shift the 128-bit value by 1 bit; if the high bit was set,
    /// XOR with the reduction polynomial 0x87.
    /// </summary>
    private static void GfMulAlpha(Span<byte> tweak)
    {
        byte carry = 0;
        for (int i = 0; i < BlockSize; i++)
        {
            byte nextCarry = (byte)((tweak[i] >> 7) & 1);
            tweak[i] = (byte)((tweak[i] << 1) | carry);
            carry = nextCarry;
        }
        if (carry != 0)
            tweak[0] ^= 0x87;
    }

    /// <summary>
    /// Verify the implementation with IEEE 1619-2007 test vector #1
    /// (all-zero keys, sector 0, all zero plaintext) and a round trip.
    /// </summary>
    public static bool SelfTest()
    {
        byte[] dataKey = new byte[16];
        byte[] tweakKey = new byte[16];
        byte[] plaintext = new byte[512];

        using var xts = new AesXts128(dataKey, tweakKey);
        byte[] ct = xts.EncryptSectors(plaintext, 0);

        byte[] expected = {
            0x91, 0x7c, 0xf6, 0x9e, 0xbd, 0x68, 0xb2, 0xec,
            0x9b, 0x9f, 0xe9, 0xa3, 0xea, 0xdd, 0xa6, 0x92
        };

        for (int i = 0; i < 16; i++)
            if (ct[i] != expected[i]) return false;

        byte[] pt = xts.DecryptSectors(ct, 0);
        for (int i = 0; i < 512; i++)
            if (pt[i] != 0) return false;

        return true;
    }

    public void Dispose() { }
}
