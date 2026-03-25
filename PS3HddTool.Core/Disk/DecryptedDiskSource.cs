using PS3HddTool.Core.Crypto;

namespace PS3HddTool.Core.Disk;

/// <summary>
/// A disk source wrapper that transparently decrypts sectors on read.
/// Wraps an encrypted IDiskSource and returns decrypted data.
/// </summary>
public sealed class DecryptedDiskSource : IDiskSource
{
    private readonly IDiskSource _source;
    private readonly AesXts128 _cipher;
    private readonly bool _applyBswap16;

    public long TotalSize => _source.TotalSize;
    public int SectorSize => _source.SectorSize;
    public long SectorCount => _source.SectorCount;
    public string Description => $"Decrypted: {_source.Description}";

    public DecryptedDiskSource(
        IDiskSource source,
        byte[] dataKey,
        byte[] tweakKey,
        bool applyBswap16 = true)
    {
        _source = source;
        _cipher = new AesXts128(dataKey, tweakKey);
        _applyBswap16 = applyBswap16;
    }

    public byte[] ReadSectors(long startSector, int count)
    {
        byte[] encrypted = _source.ReadSectors(startSector, count);

        if (_applyBswap16)
            Bswap16.SwapInPlace(encrypted);

        return _cipher.DecryptSectors(encrypted, startSector);
    }

    public byte[] ReadBytes(long offset, int count)
    {
        // Align to sector boundaries for decryption
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

        // Encrypt: plaintext → XTS encrypt → bswap16 (reverse of read path)
        byte[] encrypted = _cipher.EncryptSectors(plaintext, startSector);

        if (_applyBswap16)
            Bswap16.SwapInPlace(encrypted);

        _source.WriteSectors(startSector, encrypted);
    }

    public void WriteBytes(long offset, byte[] data)
    {
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
