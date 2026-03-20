namespace PS3HddTool.Core.Disk;

/// <summary>
/// Wraps an IDiskSource but does NOT dispose the underlying source.
/// Used when we need to create multiple DecryptedDiskSource instances
/// for key scanning without each one closing the shared source.
/// </summary>
public sealed class NonDisposingDiskSource : IDiskSource
{
    private readonly IDiskSource _inner;

    public NonDisposingDiskSource(IDiskSource inner) => _inner = inner;

    public long TotalSize => _inner.TotalSize;
    public int SectorSize => _inner.SectorSize;
    public long SectorCount => _inner.SectorCount;
    public string Description => _inner.Description;

    public byte[] ReadSectors(long startSector, int count) => _inner.ReadSectors(startSector, count);
    public byte[] ReadBytes(long offset, int count) => _inner.ReadBytes(offset, count);
    public bool CanWrite => _inner.CanWrite;
    public void WriteSectors(long startSector, byte[] data) => _inner.WriteSectors(startSector, data);
    public void WriteBytes(long offset, byte[] data) => _inner.WriteBytes(offset, data);

    // Intentionally does NOT dispose the inner source
    public void Dispose() { }
}
