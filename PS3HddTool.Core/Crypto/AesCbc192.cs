using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Security.Cryptography;

namespace PS3HddTool.Core.Crypto;

/// <summary>
/// AES-CBC-192 implementation for Fat NAND PS3 HDD encryption (CECHA/B/C/E).
/// Each 512-byte sector is independently encrypted with CBC using a zero IV.
/// </summary>
public sealed class AesCbc192 : IDisposable
{
    private readonly byte[] _key;
    public const int SectorSize = 512;

    private readonly byte[] _encryptedZeroSector;

    public AesCbc192(byte[] key)
    {
        if (key.Length != 24)
            throw new ArgumentException($"Key must be 24 bytes (192-bit). Got {key.Length} bytes.", nameof(key));

        _key = (byte[])key.Clone();

        using var aes = Aes.Create();
        aes.Key = _key;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.None;
        byte[] zeroIv = new byte[16];
        byte[] zeroPlain = new byte[SectorSize];
        _encryptedZeroSector = aes.EncryptCbc(zeroPlain, zeroIv, PaddingMode.None);
    }

    private sealed class ThreadCtx : IDisposable
    {
        public readonly Aes Aes;

        public ThreadCtx(byte[] key)
        {
            Aes = Aes.Create();
            Aes.Key = key;
            Aes.Mode = CipherMode.CBC;
            Aes.Padding = PaddingMode.None;
        }

        public void Dispose() => Aes.Dispose();
    }

    public byte[] DecryptSectors(byte[] ciphertext)
    {
        if (ciphertext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] plaintext = new byte[ciphertext.Length];
        ProcessAll(ciphertext, plaintext, encrypt: false);
        return plaintext;
    }

    public byte[] EncryptSectors(byte[] plaintext)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] ciphertext = new byte[plaintext.Length];
        ProcessAll(plaintext, ciphertext, encrypt: true);
        return ciphertext;
    }

    public void EncryptSectorsInto(byte[] plaintext, byte[] ciphertext)
        => EncryptSectorsInto(plaintext, (Span<byte>)ciphertext);

    public void DecryptSectorsInto(byte[] ciphertext, byte[] plaintext)
        => DecryptSectorsInto(ciphertext, (Span<byte>)plaintext);

    public void EncryptSectorsInto(byte[] plaintext, Span<byte> ciphertext)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");
        if (ciphertext.Length < plaintext.Length)
            throw new ArgumentException("Output buffer too small.", nameof(ciphertext));

        ProcessAll(plaintext, ciphertext.Slice(0, plaintext.Length), encrypt: true);
    }

    public void DecryptSectorsInto(byte[] ciphertext, Span<byte> plaintext)
    {
        if (ciphertext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");
        if (plaintext.Length < ciphertext.Length)
            throw new ArgumentException("Output buffer too small.", nameof(plaintext));

        ProcessAll(ciphertext, plaintext.Slice(0, ciphertext.Length), encrypt: false);
    }

    private unsafe void ProcessAll(byte[] input, Span<byte> output, bool encrypt)
    {
        int sectorCount = input.Length / SectorSize;

        if (sectorCount < 64)
        {
            using var ctx = new ThreadCtx(_key);
            ProcessSectorRange(ctx, input, output, 0, sectorCount, encrypt);
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
                () => new ThreadCtx(_key),
                (thread, _, ctx) =>
                {
                    int startS = thread * sectorsPerThread;
                    int endS = (thread == threadCount - 1) ? sectorCount : startS + sectorsPerThread;
                    Span<byte> outSpan = new Span<byte>((void*)captured, outLen);
                    ProcessSectorRange(ctx, input, outSpan, startS, endS, encrypt);
                    return ctx;
                },
                ctx => ctx.Dispose());
        }
    }

    private void ProcessSectorRange(ThreadCtx ctx, byte[] input, Span<byte> output,
        int startSector, int endSector, bool encrypt)
    {
        Span<byte> zeroIv = stackalloc byte[16];

        for (int s = startSector; s < endSector; s++)
        {
            int sectorOffset = s * SectorSize;
            ReadOnlySpan<byte> srcSector = input.AsSpan(sectorOffset, SectorSize);
            Span<byte> dstSector = output.Slice(sectorOffset, SectorSize);

            if (encrypt && IsAllZero(srcSector))
            {
                _encryptedZeroSector.AsSpan().CopyTo(dstSector);
                continue;
            }

            if (encrypt)
                ctx.Aes.EncryptCbc(srcSector, zeroIv, dstSector, PaddingMode.None);
            else
                ctx.Aes.DecryptCbc(srcSector, zeroIv, dstSector, PaddingMode.None);
        }
    }

    private static bool IsAllZero(ReadOnlySpan<byte> data)
    {
        if (Vector128.IsHardwareAccelerated)
        {
            int simdEnd = data.Length - (data.Length % 16);
            for (int i = 0; i < simdEnd; i += 16)
            {
                var v = Vector128.Create<byte>(data.Slice(i, 16));
                if (v != Vector128<byte>.Zero) return false;
            }
            for (int i = simdEnd; i < data.Length; i++)
                if (data[i] != 0) return false;
            return true;
        }
        var asLongs = MemoryMarshal.Cast<byte, long>(data);
        foreach (var v in asLongs)
            if (v != 0) return false;
        return true;
    }

    public byte[] EncryptSectorsOriginal(byte[] plaintext)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] ciphertext = new byte[plaintext.Length];
        int sectorCount = plaintext.Length / SectorSize;
        byte[] zeroIv = new byte[16];

        for (int i = 0; i < sectorCount; i++)
        {
            int offset = i * SectorSize;
            using var aes = Aes.Create();
            aes.Key = _key;
            aes.IV = zeroIv;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.None;
            using var encryptor = aes.CreateEncryptor();
            byte[] sectorData = new byte[SectorSize];
            Array.Copy(plaintext, offset, sectorData, 0, SectorSize);
            byte[] encrypted = encryptor.TransformFinalBlock(sectorData, 0, SectorSize);
            Array.Copy(encrypted, 0, ciphertext, offset, SectorSize);
        }
        return ciphertext;
    }

    public void Dispose() { }
}
