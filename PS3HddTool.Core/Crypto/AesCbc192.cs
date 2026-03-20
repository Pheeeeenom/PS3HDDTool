using System.Security.Cryptography;

namespace PS3HddTool.Core.Crypto;

/// <summary>
/// AES-CBC-192 implementation for Fat NAND PS3 HDD encryption (CECHA/B/C/E).
/// 
/// Unlike Slim/NOR models which use AES-XTS-128 with sector-based tweaks,
/// Fat NAND models use standard AES-CBC with a 192-bit (24-byte) key and 
/// a zero IV. Each sector is independently encrypted with CBC — the IV 
/// resets to zero for each sector.
/// </summary>
public sealed class AesCbc192 : IDisposable
{
    private readonly byte[] _key;
    public const int SectorSize = 512;

    public AesCbc192(byte[] key)
    {
        if (key.Length != 24)
            throw new ArgumentException($"Key must be 24 bytes (192-bit). Got {key.Length} bytes.", nameof(key));

        _key = (byte[])key.Clone();
    }

    /// <summary>
    /// Decrypt one or more sectors. Each sector uses CBC with zero IV independently.
    /// </summary>
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

    /// <summary>
    /// Encrypt one or more sectors. Each sector uses CBC with zero IV independently.
    /// </summary>
    public byte[] EncryptSectors(byte[] plaintext)
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
