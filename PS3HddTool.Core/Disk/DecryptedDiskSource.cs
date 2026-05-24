using System.Runtime.InteropServices;
using PS3HddTool.Core.Crypto;

namespace PS3HddTool.Core.Disk;

/// <summary>
/// A disk source wrapper that transparently decrypts sectors on read.
/// Wraps an encrypted IDiskSource and returns decrypted data.
/// </summary>
public sealed class DecryptedDiskSource : IDiskSource, IPipelinedDiskSource
{
    private readonly IDiskSource _source;
    private readonly AesXts128 _cipher;
    private readonly bool _applyBswap16;

    private IntPtr _alignedScratch = IntPtr.Zero;
    private int _alignedScratchSize = 0;
    private const int Alignment = 4096;

    public long TotalSize => _source.TotalSize;
    public int SectorSize => _source.SectorSize;
    public long SectorCount => _source.SectorCount;
    public string Description => $"Decrypted: {_source.Description}";

    public IDiskSource RawSource => _source;

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

    public unsafe void WriteSectors(long startSector, byte[] plaintext)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException("Data must be sector-aligned.");

        EnsureAlignedScratch(plaintext.Length);
        Span<byte> dst = new Span<byte>((void*)_alignedScratch, plaintext.Length);

        // plaintext → XTS encrypt → bswap16 all into the aligned scratch
        _cipher.EncryptSectorsInto(plaintext, dst, startSector);
        if (_applyBswap16)
            Bswap16.SwapInPlace(dst);

        _source.WriteBytes(startSector * SectorSize, (ReadOnlySpan<byte>)dst);
    }

    public void WriteBytes(long offset, byte[] data)
    {
        if (offset % SectorSize != 0 || data.Length % SectorSize != 0)
            throw new ArgumentException("Encrypted writes must be sector-aligned.");
        WriteSectors(offset / SectorSize, data);
    }

    public void WriteBytes(long offset, ReadOnlySpan<byte> data)
    {
        WriteBytes(offset, data.ToArray());
    }

    public void EncryptForWrite(byte[] plaintext, int byteCount, long offset, Span<byte> ciphertext)
    {
        if (byteCount % SectorSize != 0)
            throw new ArgumentException("Byte count must be sector aligned.");
        if (offset % SectorSize != 0)
            throw new ArgumentException("Offset must be sector aligned.");
        if (ciphertext.Length < byteCount)
            throw new ArgumentException("Output span too small.", nameof(ciphertext));

        long startSector = offset / SectorSize;

        if (byteCount == plaintext.Length)
        {
            _cipher.EncryptSectorsInto(plaintext, ciphertext, startSector);
        }
        else
        {
            byte[] sized = new byte[byteCount];
            Buffer.BlockCopy(plaintext, 0, sized, 0, byteCount);
            _cipher.EncryptSectorsInto(sized, ciphertext, startSector);
        }

        if (_applyBswap16)
            Bswap16.SwapInPlace(ciphertext.Slice(0, byteCount));
    }

    private unsafe void EnsureAlignedScratch(int size)
    {
        if (_alignedScratch == IntPtr.Zero || _alignedScratchSize < size)
        {
            if (_alignedScratch != IntPtr.Zero)
                NativeMemory.AlignedFree((void*)_alignedScratch);
            _alignedScratch = (IntPtr)NativeMemory.AlignedAlloc((nuint)size, Alignment);
            _alignedScratchSize = size;
        }
    }

    public unsafe void Dispose()
    {
        _cipher.Dispose();
        _source.Dispose();
        if (_alignedScratch != IntPtr.Zero)
        {
            NativeMemory.AlignedFree((void*)_alignedScratch);
            _alignedScratch = IntPtr.Zero;
        }
    }
}
