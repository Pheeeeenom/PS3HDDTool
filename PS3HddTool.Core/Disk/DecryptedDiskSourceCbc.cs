using System.Runtime.InteropServices;
using PS3HddTool.Core.Crypto;

namespace PS3HddTool.Core.Disk;

/// <summary>
/// Decrypted disk source using AES-CBC-192 for Fat NAND PS3 models.
/// No bswap16 needed. No tweak key. Just CBC with zero IV per sector.
/// </summary>
public sealed class DecryptedDiskSourceCbc : IDiskSource, IPipelinedDiskSource
{
    private readonly IDiskSource _source;
    private readonly AesCbc192 _cipher;
    private readonly bool _applyBswap16;
    
    private IntPtr _alignedScratch = IntPtr.Zero;
    private int _alignedScratchSize = 0;
    private const int Alignment = 4096;

    public long TotalSize => _source.TotalSize;
    public int SectorSize => _source.SectorSize;
    public long SectorCount => _source.SectorCount;
    public string Description => $"Decrypted (CBC-192): {_source.Description}";

    public IDiskSource RawSource => _source;

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

    public unsafe void WriteSectors(long startSector, byte[] plaintext)
    {
        if (plaintext.Length % SectorSize != 0)
            throw new ArgumentException("Data must be sector-aligned.");

        EnsureAlignedScratch(plaintext.Length);
        Span<byte> dst = new Span<byte>((void*)_alignedScratch, plaintext.Length);

        // plaintext → CBC encrypt → (optional bswap16) all into the aligned scratch
        _cipher.EncryptSectorsInto(plaintext, dst);
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
        if (ciphertext.Length < byteCount)
            throw new ArgumentException("Output span too small.", nameof(ciphertext));

        if (byteCount == plaintext.Length)
        {
            _cipher.EncryptSectorsInto(plaintext, ciphertext);
        }
        else
        {
            byte[] sized = new byte[byteCount];
            Buffer.BlockCopy(plaintext, 0, sized, 0, byteCount);
            _cipher.EncryptSectorsInto(sized, ciphertext);
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
