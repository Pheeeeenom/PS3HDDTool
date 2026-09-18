using PS3HddTool.Core.Crypto;

namespace PS3HddTool.Core.Disk;

/// <summary>
/// A bounded, read-only PS4 partition. Disk LBAs and XTS sector numbers are independent.
/// Borrows the raw disk; disposing this view never closes the raw source.
/// </summary>
public sealed class Ps4PartitionSource : IDiskSource
{
    private readonly IDiskSource _source;
    private readonly AesXts128 _cipher;
    private readonly long _startSector;
    private readonly long _ivOffset;
    private bool _disposed;
    public long TotalSize { get; }
    public int SectorSize => 512;
    public long SectorCount => TotalSize / SectorSize;
    public string Description { get; }
    public bool CanWrite => false;

    public Ps4PartitionSource(IDiskSource source, Ps4Partition partition, byte[] key, long ivOffset)
    {
        if (key.Length != 32) throw new ArgumentException("PS4 EAP HDD key must be 32 bytes.", nameof(key));
        if (source.SectorSize != 512 || partition.StartSector < 0 || partition.SectorCount <= 0 ||
            partition.StartSector > source.SectorCount - partition.SectorCount ||
            ivOffset < 0 || ivOffset > long.MaxValue - partition.SectorCount)
            throw new ArgumentOutOfRangeException(nameof(partition));
        _source = source;
        _startSector = partition.StartSector;
        _ivOffset = ivOffset;
        TotalSize = checked(partition.SectorCount * 512);
        Description = $"PS4 {partition.Name} (read-only)";
        _cipher = new AesXts128(key[..16], key[16..]);
    }

    public byte[] ReadSectors(long startSector, int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (startSector < 0 || count < 0 || startSector > SectorCount - count)
            throw new ArgumentOutOfRangeException(nameof(startSector), "Read exceeds the PS4 partition.");
        byte[] ciphertext = _source.ReadSectors(_startSector + startSector, count);
        if (ciphertext.Length != checked(count * 512)) throw new EndOfStreamException("Truncated partition read.");
        // No PS3 bswap16: PS4 ciphertext is consumed in its original byte order.
        return _cipher.DecryptSectors(ciphertext, _ivOffset + startSector);
    }

    public byte[] ReadBytes(long offset, int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (offset < 0 || count < 0 || offset > TotalSize - count)
            throw new ArgumentOutOfRangeException(nameof(offset), "Read exceeds the PS4 partition.");
        if (count == 0) return Array.Empty<byte>();
        int delta = (int)(offset % 512);
        int sectors = checked((int)(((long)delta + count + 511) / 512));
        byte[] plaintext = ReadSectors(offset / 512, sectors);
        return plaintext.AsSpan(delta, count).ToArray();
    }

    public void WriteSectors(long startSector, byte[] data) => throw ReadOnly();
    public void WriteBytes(long offset, byte[] data) => throw ReadOnly();
    public void WriteBytes(long offset, ReadOnlySpan<byte> data) => throw ReadOnly();
    private static NotSupportedException ReadOnly() => new("PS4 partitions are read-only. Extract files to your computer instead.");
    public void Dispose() { _disposed = true; _cipher.Dispose(); }
}
