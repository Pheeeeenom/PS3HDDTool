using System.Security.Cryptography;
using System.Threading.Tasks;

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
        aes.IV = new byte[16];
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.None;
        using var enc = aes.CreateEncryptor();
        _encryptedZeroSector = enc.TransformFinalBlock(new byte[SectorSize], 0, SectorSize);
    }

    public byte[] DecryptSectors(byte[] ciphertext)
    {
        if (ciphertext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] plaintext = new byte[ciphertext.Length];
        int sectorCount = ciphertext.Length / SectorSize;
        byte[] zeroIv = new byte[16];

        for (int i = 0; i < sectorCount; i++)
        {
            int offset = i * SectorSize;
            using var aes = Aes.Create();
            aes.Key = _key;
            aes.IV = zeroIv;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.None;
            using var decryptor = aes.CreateDecryptor();
            byte[] sectorData = new byte[SectorSize];
            Array.Copy(ciphertext, offset, sectorData, 0, SectorSize);
            byte[] decrypted = decryptor.TransformFinalBlock(sectorData, 0, SectorSize);
            Array.Copy(decrypted, 0, plaintext, offset, SectorSize);
        }
        return plaintext;
    }

    public byte[] EncryptSectors(byte[] plaintext)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] ciphertext = new byte[plaintext.Length];
        int sectorCount = plaintext.Length / SectorSize;
        int blocksPerSector = SectorSize / 16;

        if (sectorCount < 64)
        {
            EncryptSectorRange(plaintext, ciphertext, 0, sectorCount, blocksPerSector);
            return ciphertext;
        }

        int threadCount = Math.Min(Environment.ProcessorCount, sectorCount / 32);
        if (threadCount < 2) threadCount = 2;
        int sectorsPerThread = sectorCount / threadCount;

        Parallel.For(0, threadCount, thread =>
        {
            int startSector = thread * sectorsPerThread;
            int endSector = (thread == threadCount - 1) ? sectorCount : startSector + sectorsPerThread;
            EncryptSectorRange(plaintext, ciphertext, startSector, endSector, blocksPerSector);
        });

        return ciphertext;
    }

    private void EncryptSectorRange(byte[] plaintext, byte[] ciphertext, int startSector, int endSector, int blocksPerSector)
    {
        using var aes = Aes.Create();
        aes.Key = _key;
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.None;
        using var encryptor = aes.CreateEncryptor();

        byte[] xorBlock = new byte[16];

        for (int s = startSector; s < endSector; s++)
        {
            int sectorOffset = s * SectorSize;

            // Skip encryption for zero-filled sectors
            bool isZero = true;
            for (int z = 0; z < SectorSize; z += 8)
            {
                if (BitConverter.ToInt64(plaintext, sectorOffset + z) != 0)
                { isZero = false; break; }
            }
            if (isZero)
            {
                Buffer.BlockCopy(_encryptedZeroSector, 0, ciphertext, sectorOffset, SectorSize);
                continue;
            }

            Array.Clear(xorBlock, 0, 16);
            for (int bl = 0; bl < blocksPerSector; bl++)
            {
                int blockOffset = sectorOffset + bl * 16;
                long p0 = BitConverter.ToInt64(plaintext, blockOffset);
                long p1 = BitConverter.ToInt64(plaintext, blockOffset + 8);
                long x0 = BitConverter.ToInt64(xorBlock, 0);
                long x1 = BitConverter.ToInt64(xorBlock, 8);
                BitConverter.TryWriteBytes(xorBlock.AsSpan(0), x0 ^ p0);
                BitConverter.TryWriteBytes(xorBlock.AsSpan(8), x1 ^ p1);
                encryptor.TransformBlock(xorBlock, 0, 16, xorBlock, 0);
                Buffer.BlockCopy(xorBlock, 0, ciphertext, blockOffset, 16);
            }
        }
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
