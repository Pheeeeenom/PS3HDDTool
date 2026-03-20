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

    public AesXts128(byte[] dataKey, byte[] tweakKey)
    {
        if (dataKey.Length != 16)
            throw new ArgumentException("Data key must be 16 bytes.", nameof(dataKey));
        if (tweakKey.Length != 16)
            throw new ArgumentException("Tweak key must be 16 bytes.", nameof(tweakKey));

        _dataKey = (byte[])dataKey.Clone();
        _tweakKey = (byte[])tweakKey.Clone();
    }

    public byte[] DecryptSectors(byte[] ciphertext, long startSectorNumber)
    {
        if (ciphertext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] plaintext = new byte[ciphertext.Length];
        int sectorCount = ciphertext.Length / SectorSize;

        for (int i = 0; i < sectorCount; i++)
        {
            DecryptSector(ciphertext, i * SectorSize, plaintext, i * SectorSize,
                          startSectorNumber + i);
        }

        return plaintext;
    }

    private void DecryptSector(byte[] input, int inputOffset, byte[] output, int outputOffset,
                                long sectorNumber)
    {
        // Step 1: Build tweak — encrypt sector number with tweak key
        byte[] tweakPlain = new byte[BlockSize];
        // Sector number as little-endian 64-bit in a 128-bit block
        for (int i = 0; i < 8; i++)
            tweakPlain[i] = (byte)(sectorNumber >> (i * 8));

        byte[] tweak = AesEcbEncryptBlock(_tweakKey, tweakPlain);

        // Step 2: Process each 16-byte block
        int blocksPerSector = SectorSize / BlockSize;
        byte[] block = new byte[BlockSize];

        for (int j = 0; j < blocksPerSector; j++)
        {
            int off = inputOffset + j * BlockSize;

            // XOR ciphertext with tweak
            for (int k = 0; k < BlockSize; k++)
                block[k] = (byte)(input[off + k] ^ tweak[k]);

            // AES-ECB decrypt
            byte[] decrypted = AesEcbDecryptBlock(_dataKey, block);

            // XOR with tweak again
            int outOff = outputOffset + j * BlockSize;
            for (int k = 0; k < BlockSize; k++)
                output[outOff + k] = (byte)(decrypted[k] ^ tweak[k]);

            // Advance tweak: multiply by alpha in GF(2^128)
            GfMul(tweak);
        }
    }

    public byte[] EncryptSectors(byte[] plaintext, long startSectorNumber)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException($"Length must be a multiple of {SectorSize}.");

        byte[] ciphertext = new byte[plaintext.Length];
        int sectorCount = plaintext.Length / SectorSize;

        for (int i = 0; i < sectorCount; i++)
        {
            EncryptSector(plaintext, i * SectorSize, ciphertext, i * SectorSize,
                          startSectorNumber + i);
        }

        return ciphertext;
    }

    private void EncryptSector(byte[] input, int inputOffset, byte[] output, int outputOffset,
                                long sectorNumber)
    {
        byte[] tweakPlain = new byte[BlockSize];
        for (int i = 0; i < 8; i++)
            tweakPlain[i] = (byte)(sectorNumber >> (i * 8));

        byte[] tweak = AesEcbEncryptBlock(_tweakKey, tweakPlain);

        int blocksPerSector = SectorSize / BlockSize;
        byte[] block = new byte[BlockSize];

        for (int j = 0; j < blocksPerSector; j++)
        {
            int off = inputOffset + j * BlockSize;

            for (int k = 0; k < BlockSize; k++)
                block[k] = (byte)(input[off + k] ^ tweak[k]);

            byte[] encrypted = AesEcbEncryptBlock(_dataKey, block);

            int outOff = outputOffset + j * BlockSize;
            for (int k = 0; k < BlockSize; k++)
                output[outOff + k] = (byte)(encrypted[k] ^ tweak[k]);

            GfMul(tweak);
        }
    }

    /// <summary>
    /// AES-ECB encrypt a single 16-byte block. Fresh transform each call.
    /// </summary>
    private static byte[] AesEcbEncryptBlock(byte[] key, byte[] block)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.None;
        using var enc = aes.CreateEncryptor();
        return enc.TransformFinalBlock(block, 0, BlockSize);
    }

    /// <summary>
    /// AES-ECB decrypt a single 16-byte block. Fresh transform each call.
    /// </summary>
    private static byte[] AesEcbDecryptBlock(byte[] key, byte[] block)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.None;
        using var dec = aes.CreateDecryptor();
        return dec.TransformFinalBlock(block, 0, BlockSize);
    }

    /// <summary>
    /// GF(2^128) multiply by alpha (x). 
    /// Left-shift the 128-bit value by 1 bit; if the high bit was set,
    /// XOR with the reduction polynomial 0x87.
    /// </summary>
    private static void GfMul(byte[] tweak)
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
    /// Verify the implementation with a known test vector.
    /// IEEE 1619-2007 test vector #1 (all-zero key, sector 0).
    /// </summary>
    public static bool SelfTest()
    {
        // IEEE 1619 vector 1: keys all zero, sector 0, plaintext all zero
        // Expected ciphertext first 16 bytes: 917cf69ebd68b2ec 9b9fe9a3eadda692
        byte[] dataKey = new byte[16];
        byte[] tweakKey = new byte[16];
        byte[] plaintext = new byte[512]; // all zeros

        using var xts = new AesXts128(dataKey, tweakKey);
        byte[] ct = xts.EncryptSectors(plaintext, 0);

        // Check first 16 bytes of ciphertext
        byte[] expected = {
            0x91, 0x7c, 0xf6, 0x9e, 0xbd, 0x68, 0xb2, 0xec,
            0x9b, 0x9f, 0xe9, 0xa3, 0xea, 0xdd, 0xa6, 0x92
        };

        for (int i = 0; i < 16; i++)
        {
            if (ct[i] != expected[i]) return false;
        }

        // Verify round-trip
        byte[] pt = xts.DecryptSectors(ct, 0);
        for (int i = 0; i < 512; i++)
        {
            if (pt[i] != 0) return false;
        }

        return true;
    }

    public void Dispose() { }
}
