using PS3HddTool.Core.Crypto;

namespace PS3HddTool.Core.Disk;

/// <summary>
/// Decrypted disk source using AES-CBC-192 for Fat NAND PS3 models.
/// No bswap16 needed. No tweak key. Just CBC with zero IV per sector.
/// </summary>
public sealed class DecryptedDiskSourceCbc : IDiskSource
{
    private readonly IDiskSource _source;
    private readonly AesCbc192 _cipher;
    private readonly bool _applyBswap16;

    public long TotalSize => _source.TotalSize;
    public int SectorSize => _source.SectorSize;
    public long SectorCount => _source.SectorCount;
    public string Description => $"Decrypted (CBC-192): {_source.Description}";

    public DecryptedDiskSourceCbc(IDiskSource source, byte[] key, bool applyBswap16 = false)
    {
        _source = source;
        _cipher = new AesCbc192(key);
        _applyBswap16 = applyBswap16;
    }

    public byte[] ReadSectors(long startSector, int count)
    {
        // Read in chunks to avoid issues with large reads on physical drives
        const int MaxSectorsPerRead = 256; // 128KB chunks
        
        byte[] allData = new byte[count * SectorSize];
        int remaining = count;
        int offset = 0;

        while (remaining > 0)
        {
            int chunk = Math.Min(remaining, MaxSectorsPerRead);
            long currentSector = startSector + (offset / SectorSize);

            byte[] encrypted = _source.ReadSectors(currentSector, chunk);

            if (_applyBswap16)
                Bswap16.SwapInPlace(encrypted);

            byte[] decrypted = _cipher.DecryptSectors(encrypted);
            Array.Copy(decrypted, 0, allData, offset, chunk * SectorSize);

            offset += chunk * SectorSize;
            remaining -= chunk;
        }

        return allData;
    }

    public byte[] ReadBytes(long offset, int count)
    {
        long startSector = offset / SectorSize;
        int sectorOffset = (int)(offset % SectorSize);
        int sectorsNeeded = (int)((sectorOffset + count + SectorSize - 1) / SectorSize);

        byte[] decryptedSectors = ReadSectors(startSector, sectorsNeeded);
        byte[] result = new byte[count];
        Array.Copy(decryptedSectors, sectorOffset, result, 0, count);
        return result;
    }

    public bool CanWrite => _source.CanWrite;

    public void WriteSectors(long startSector, byte[] plaintext)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException("Data must be sector-aligned.");

        // Encrypt: plaintext → CBC encrypt → bswap16 (reverse of read path)
        byte[] encrypted = _cipher.EncryptSectors(plaintext);

        if (_applyBswap16)
            Bswap16.SwapInPlace(encrypted);

        _source.WriteSectors(startSector, encrypted);
    }

    public void WriteBytes(long offset, byte[] data)
    {
        // Must be sector-aligned for encryption to work
        if (offset % SectorSize != 0 || data.Length % SectorSize != 0)
            throw new ArgumentException("Encrypted writes must be sector-aligned.");

        long startSector = offset / SectorSize;
        WriteSectors(startSector, data);
    }

    public void Dispose()
    {
        _cipher.Dispose();
        _source.Dispose();
    }
}
