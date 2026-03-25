using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using PS3HddTool.Core.FileSystem;

namespace PS3HddTool.Core.Pkg;

/// <summary>
/// Reads and extracts PS3 NPDRM PKG files.
/// Supports both retail (finalized 0x80) and debug packages.
/// </summary>
public class Ps3PkgReader : IDisposable
{
    // PS3 global AES key for retail PKG decryption
    private static readonly byte[] PS3_AES_KEY = {
        0x2E, 0x7B, 0x71, 0xD7, 0xC9, 0xC9, 0xA1, 0x4E,
        0xA3, 0x22, 0x1F, 0x18, 0x88, 0x28, 0xB8, 0xF8
    };

    private readonly Stream _stream;
    private readonly BinaryReader _reader;
    private readonly bool _ownsStream;
    private Aes? _aes;
    private ICryptoTransform? _aesEncryptor;
    private Action<string>? _log;
    
    // Pre-allocated per-thread AES encryptors for parallel decrypt
    private Aes[]? _threadAes;
    private ICryptoTransform[]? _threadEncryptors;
    private int _threadCount;

    // Header fields
    public byte[] Magic { get; private set; } = new byte[4];
    public byte PkgRevision { get; private set; }
    public byte PkgType { get; private set; } // 0x01 = PS3, 0x02 = PSP
    public uint MetadataOffset { get; private set; }
    public uint MetadataCount { get; private set; }
    public uint HeaderSize { get; private set; }
    public uint DataOffset { get; private set; }
    public ulong DataSize { get; private set; }
    public string ContentId { get; private set; } = "";
    public byte[] FileKey { get; private set; } = new byte[16];
    public uint FileCount { get; private set; }
    public ulong TotalSize { get; private set; }

    // Parsed entries
    public List<PkgEntry> Entries { get; private set; } = new();

    // PARAM.SFO data (parsed after reading entries)
    public string TitleId { get; private set; } = "";
    public string Title { get; private set; } = "";

    public Ps3PkgReader(string filePath, Action<string>? log = null)
    {
        _stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        _reader = new BinaryReader(_stream);
        _ownsStream = true;
        _log = log;
        ParseHeader();
        InitAes();
        ParseFileTable();
        FindTitleId();
    }

    public Ps3PkgReader(Stream stream)
    {
        _stream = stream;
        _reader = new BinaryReader(_stream);
        _ownsStream = false;
        ParseHeader();
        InitAes();
        ParseFileTable();
        FindTitleId();
    }

    private void InitAes()
    {
        _aes = Aes.Create();
        _aes.Key = PS3_AES_KEY;
        _aes.Mode = CipherMode.ECB;
        _aes.Padding = PaddingMode.None;
        _aesEncryptor = _aes.CreateEncryptor();
        
        // Pre-allocate per-thread AES encryptors
        _threadCount = Environment.ProcessorCount;
        _threadAes = new Aes[_threadCount];
        _threadEncryptors = new ICryptoTransform[_threadCount];
        for (int i = 0; i < _threadCount; i++)
        {
            _threadAes[i] = Aes.Create();
            _threadAes[i].Key = PS3_AES_KEY;
            _threadAes[i].Mode = CipherMode.ECB;
            _threadAes[i].Padding = PaddingMode.None;
            _threadEncryptors[i] = _threadAes[i].CreateEncryptor();
        }
    }

    private void ParseHeader()
    {
        _stream.Seek(0, SeekOrigin.Begin);
        Magic = _reader.ReadBytes(4);

        // Check magic: 0x7F 'P' 'K' 'G'
        if (Magic[0] != 0x7F || Magic[1] != 0x50 || Magic[2] != 0x4B || Magic[3] != 0x47)
            throw new InvalidDataException("Not a valid PS3 PKG file (bad magic).");

        // 0x04: revision/finalized byte
        PkgRevision = _reader.ReadByte();

        // 0x05-0x06: skip
        _reader.ReadBytes(2);

        // 0x07: pkg type
        PkgType = _reader.ReadByte();
        if (PkgType != 0x01)
            throw new InvalidDataException($"Not a PS3 PKG (type=0x{PkgType:X2}). Only PS3 (0x01) is supported.");

        // 0x08: metadata offset (u32 BE)
        _stream.Seek(0x08, SeekOrigin.Begin);
        MetadataOffset = ReadU32BE();

        // 0x0C: metadata count (u32 BE)
        MetadataCount = ReadU32BE();

        // 0x10: header size (u32 BE)
        HeaderSize = ReadU32BE();

        // 0x14-0x17: item count (u32 BE)
        FileCount = ReadU32BE();

        // 0x18: total pkg size (u64 BE)
        TotalSize = ReadU64BE();

        // 0x20: data offset (u64 BE)
        _stream.Seek(0x20, SeekOrigin.Begin);
        DataOffset = (uint)ReadU64BE();

        // 0x28: data size (u64 BE)
        DataSize = ReadU64BE();

        // 0x30: content ID (36 bytes ASCII)
        _stream.Seek(0x30, SeekOrigin.Begin);
        byte[] contentIdBytes = _reader.ReadBytes(48);
        ContentId = Encoding.ASCII.GetString(contentIdBytes).TrimEnd('\0');

        // 0x70: file key (16 bytes)
        _stream.Seek(0x70, SeekOrigin.Begin);
        FileKey = _reader.ReadBytes(16);
    }

    private void ParseFileTable()
    {
        Entries.Clear();

        // The file table is at the start of the encrypted data region
        // Each entry is 32 bytes:
        // 0-3:   name_offset (u32 BE, relative to data start)
        // 4-7:   name_size (u32 BE)
        // 8-11:  padding
        // 12-15: data_offset (u32 BE, relative to data start)
        // 16-19: padding
        // 20-23: data_size (u32 BE)
        // 24:    content_type (0x80/0x00 = PS3, 0x90 = PSP)
        // 25-26: padding
        // 27:    file_type (0x01 = NPDRM, 0x03 = raw, 0x04 = directory)
        // 28-31: padding

        int tableSize = (int)(FileCount * 32);
        byte[] encryptedTable = new byte[tableSize];

        // Read encrypted file table from data region
        _stream.Seek(DataOffset, SeekOrigin.Begin);
        _stream.Read(encryptedTable, 0, tableSize);

        // Decrypt the file table
        byte[] decryptedTable = DecryptData(encryptedTable, 0, tableSize);

        for (int i = 0; i < (int)FileCount; i++)
        {
            int off = i * 32;

            uint nameOffset = BinaryPrimitives.ReadUInt32BigEndian(decryptedTable.AsSpan(off));
            uint nameSize = BinaryPrimitives.ReadUInt32BigEndian(decryptedTable.AsSpan(off + 4));
            uint dataOffset = BinaryPrimitives.ReadUInt32BigEndian(decryptedTable.AsSpan(off + 12));
            uint dataSize = BinaryPrimitives.ReadUInt32BigEndian(decryptedTable.AsSpan(off + 20));
            byte contentType = decryptedTable[off + 24];
            byte fileType = decryptedTable[off + 27];

            // Decrypt the file name
            byte[] encName = new byte[nameSize];
            _stream.Seek(DataOffset + nameOffset, SeekOrigin.Begin);
            _stream.Read(encName, 0, (int)nameSize);
            byte[] decName = DecryptData(encName, (long)nameOffset, (int)nameSize);
            string name = Encoding.ASCII.GetString(decName, 0, (int)nameSize).TrimEnd('\0');

            bool isDir = (fileType == 0x04 && dataSize == 0);

            Entries.Add(new PkgEntry
            {
                Name = name,
                NameOffset = nameOffset,
                NameSize = nameSize,
                DataOffset = dataOffset,
                DataSize = dataSize,
                ContentType = contentType,
                FileType = fileType,
                IsDirectory = isDir
            });
        }
    }

    private void FindTitleId()
    {
        // Try to get TITLE_ID from Content ID first (format: XX0000-TITLEID00-...)
        if (ContentId.Length >= 16)
        {
            // Content ID format: XX####-TITLEID_-XXXXXXXXXXXXXXXX
            // TITLE_ID is typically at position 7, length 9 (e.g. BLUS12345)
            int dash1 = ContentId.IndexOf('-');
            if (dash1 >= 0 && dash1 + 10 < ContentId.Length)
            {
                int dash2 = ContentId.IndexOf('-', dash1 + 1);
                if (dash2 > dash1)
                {
                    TitleId = ContentId.Substring(dash1 + 1, dash2 - dash1 - 1);
                }
            }
        }

        // Also try to find and parse PARAM.SFO from the entries
        foreach (var entry in Entries)
        {
            if (entry.Name.Equals("PARAM.SFO", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    byte[] sfoData = ExtractEntryToMemory(entry);
                    var sfo = ParamSfo.Parse(sfoData);
                    if (sfo != null)
                    {
                        if (!string.IsNullOrEmpty(sfo.TitleId))
                            TitleId = sfo.TitleId;
                        if (!string.IsNullOrEmpty(sfo.Title))
                            Title = sfo.Title;
                    }
                }
                catch { }
                break;
            }
        }
    }

    /// <summary>
    /// Decrypt PKG data using AES-ECB counter mode XOR (single-threaded, for small data).
    /// </summary>
    private byte[] DecryptData(byte[] encrypted, long dataRelativeOffset, int size)
    {
        int alignedSize = size;
        if (alignedSize % 16 != 0)
            alignedSize = ((alignedSize / 16) + 1) * 16;

        long startBlock = dataRelativeOffset / 16;
        int blocks = alignedSize / 16;

        byte[] counterBlock = new byte[alignedSize];
        FillCounterBlocks(FileKey, startBlock, counterBlock, 0, blocks);

        byte[] keystream = _aesEncryptor!.TransformFinalBlock(counterBlock, 0, counterBlock.Length);

        byte[] result = new byte[size];
        for (int i = 0; i < size; i++)
            result[i] = (byte)(encrypted[i] ^ keystream[i]);

        return result;
    }

    /// <summary>
    /// Parallel version of DecryptData. Splits work across threads for large buffers.
    /// Each thread builds its own counter block segment and AES-encrypts independently.
    /// </summary>
    /// <summary>
    /// Fast counter block fill. Precomputes the upper 8 bytes of FileKey+startCounter,
    /// then just writes upper + incrementing lower for each block. Avoids Array.Copy.
    /// </summary>
    private static void FillCounterBlocks(byte[] fileKey, long startBlock, byte[] dest, int destOffset, int blockCount)
    {
        // Compute FileKey as two big-endian uint64s
        ulong keyHi = BinaryPrimitives.ReadUInt64BigEndian(fileKey.AsSpan(0));
        ulong keyLo = BinaryPrimitives.ReadUInt64BigEndian(fileKey.AsSpan(8));

        // Add startBlock to the 128-bit counter
        ulong lo = keyLo + (ulong)startBlock;
        ulong hi = keyHi;
        if (lo < keyLo) hi++; // carry

        for (int b = 0; b < blockCount; b++)
        {
            int off = destOffset + b * 16;
            BinaryPrimitives.WriteUInt64BigEndian(dest.AsSpan(off), hi);
            BinaryPrimitives.WriteUInt64BigEndian(dest.AsSpan(off + 8), lo);

            lo++;
            if (lo == 0) hi++; // carry on overflow
        }
    }

    /// <summary>
    /// Parallel decrypt with per-phase timing output.
    /// Optimized: fast counter fill, pre-allocated AES encryptors, in-place XOR.
    /// </summary>
    private byte[] DecryptDataParallelTimed(byte[] encrypted, long dataRelativeOffset, int size,
        out long counterTicks, out long aesTicks, out long xorTicks)
    {
        int alignedSize = size;
        if (alignedSize % 16 != 0)
            alignedSize = ((alignedSize / 16) + 1) * 16;

        long startBlock = dataRelativeOffset / 16;
        int totalBlocks = alignedSize / 16;

        byte[] result = new byte[alignedSize];

        int threadCount = Math.Min(_threadCount, Math.Max(1, totalBlocks / 1024));
        if (threadCount < 2) threadCount = 1;

        int blocksPerThread = totalBlocks / threadCount;

        long ct0 = System.Diagnostics.Stopwatch.GetTimestamp();

        Parallel.For(0, threadCount, thread =>
        {
            int blockStart = thread * blocksPerThread;
            int blockEnd = (thread == threadCount - 1) ? totalBlocks : blockStart + blocksPerThread;
            int segmentBlocks = blockEnd - blockStart;
            int segmentBytes = segmentBlocks * 16;
            int byteOffset = blockStart * 16;

            // Fast counter fill
            byte[] counterSeg = new byte[segmentBytes];
            FillCounterBlocks(FileKey, startBlock + blockStart, counterSeg, 0, segmentBlocks);

            // Use pre-allocated encryptor for this thread
            byte[] keystreamSeg = _threadEncryptors![thread].TransformFinalBlock(counterSeg, 0, counterSeg.Length);

            // XOR in same thread
            int xorEnd = Math.Min(byteOffset + segmentBytes, size);
            for (int i = byteOffset; i < xorEnd; i++)
                result[i] = (byte)(encrypted[i] ^ keystreamSeg[i - byteOffset]);
        });

        long ct1 = System.Diagnostics.Stopwatch.GetTimestamp();

        counterTicks = 0;
        aesTicks = ct1 - ct0;
        xorTicks = 0; // XOR is now inside the parallel block

        // Trim to actual size if needed
        if (result.Length != size)
        {
            byte[] trimmed = new byte[size];
            Buffer.BlockCopy(result, 0, trimmed, 0, size);
            return trimmed;
        }
        return result;
    }

    /// <summary>
    /// Extract a single entry to memory.
    /// </summary>
    public byte[] ExtractEntryToMemory(PkgEntry entry)
    {
        if (entry.IsDirectory || entry.DataSize == 0)
            return Array.Empty<byte>();

        byte[] encrypted = new byte[entry.DataSize];
        _stream.Seek(DataOffset + entry.DataOffset, SeekOrigin.Begin);
        _stream.Read(encrypted, 0, (int)entry.DataSize);

        return DecryptData(encrypted, entry.DataOffset, (int)entry.DataSize);
    }

    /// <summary>
    /// Extract a single entry to a file on disk.
    /// Double-buffered: writes previous chunk while reading+decrypting current.
    /// </summary>
    public void ExtractEntryToFile(PkgEntry entry, string outputPath)
    {
        if (entry.IsDirectory)
        {
            Directory.CreateDirectory(outputPath);
            return;
        }

        // Ensure parent directory exists
        string? dir = Path.GetDirectoryName(outputPath);
        if (dir != null && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        const int CHUNK_SIZE = 1024 * 1024 * 64; // 64MB
        long remaining = entry.DataSize;
        long fileOffset = entry.DataOffset;

        // Timing
        long readTicks = 0, aesTicks = 0, xorTicks = 0, writeTicks = 0, waitTicks = 0;

        using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 1024);

        Task? pendingWrite = null;
        byte[]? pendingData = null;
        int pendingSize = 0;

        while (remaining > 0)
        {
            int toRead = (int)Math.Min(CHUNK_SIZE, remaining);

            long t0 = System.Diagnostics.Stopwatch.GetTimestamp();

            byte[] encrypted = new byte[toRead];
            _stream.Seek(DataOffset + fileOffset, SeekOrigin.Begin);
            _stream.Read(encrypted, 0, toRead);

            long t1 = System.Diagnostics.Stopwatch.GetTimestamp();

            byte[] decrypted = DecryptDataParallelTimed(encrypted, fileOffset, toRead,
                out long ctrTicks, out long aeTicks, out long xrTicks);

            long t2 = System.Diagnostics.Stopwatch.GetTimestamp();

            // Wait for previous write to finish before starting new one
            if (pendingWrite != null)
            {
                pendingWrite.Wait();
            }

            long t3 = System.Diagnostics.Stopwatch.GetTimestamp();

            // Kick off async write for this chunk
            pendingData = decrypted;
            pendingSize = toRead;
            var capturedData = pendingData;
            var capturedSize = pendingSize;
            var capturedStream = outStream;
            pendingWrite = Task.Run(() => capturedStream.Write(capturedData, 0, capturedSize));

            readTicks += t1 - t0;
            aesTicks += aeTicks;
            xorTicks += xrTicks;
            waitTicks += t3 - t2;

            fileOffset += toRead;
            remaining -= toRead;
        }

        // Wait for final write
        long tw0 = System.Diagnostics.Stopwatch.GetTimestamp();
        pendingWrite?.Wait();
        long tw1 = System.Diagnostics.Stopwatch.GetTimestamp();
        waitTicks += tw1 - tw0;

        double freq = System.Diagnostics.Stopwatch.Frequency;
        if (entry.DataSize > 1024 * 1024)
        {
            _log?.Invoke($"  PKG TIMING [{entry.Name}]: read={readTicks * 1000 / freq:F0}ms, aes={aesTicks * 1000 / freq:F0}ms, xor={xorTicks * 1000 / freq:F0}ms, write_wait={waitTicks * 1000 / freq:F0}ms, size={entry.DataSize / (1024 * 1024)}MB");
        }
    }

    /// <summary>
    /// Extract all entries to a directory, organized by TITLE_ID.
    /// Returns the output directory path.
    /// </summary>
    public string ExtractAll(string outputDir, Action<string, double>? progress = null)
    {
        string gameDir = Path.Combine(outputDir, string.IsNullOrEmpty(TitleId) ? "UNKNOWN" : TitleId);
        Directory.CreateDirectory(gameDir);

        int total = Entries.Count;
        for (int i = 0; i < total; i++)
        {
            var entry = Entries[i];
            string safeName = entry.Name.Replace('/', Path.DirectorySeparatorChar);
            string fullPath = Path.Combine(gameDir, safeName);

            progress?.Invoke(entry.Name, (double)(i + 1) / total * 100);

            if (entry.IsDirectory)
            {
                Directory.CreateDirectory(fullPath);
            }
            else
            {
                ExtractEntryToFile(entry, fullPath);
            }
        }

        return gameDir;
    }

    private uint ReadU32BE()
    {
        byte[] b = _reader.ReadBytes(4);
        return BinaryPrimitives.ReadUInt32BigEndian(b);
    }

    private ulong ReadU64BE()
    {
        byte[] b = _reader.ReadBytes(8);
        return BinaryPrimitives.ReadUInt64BigEndian(b);
    }

    public void Dispose()
    {
        _aesEncryptor?.Dispose();
        _aes?.Dispose();
        if (_threadEncryptors != null)
            foreach (var enc in _threadEncryptors) enc?.Dispose();
        if (_threadAes != null)
            foreach (var aes in _threadAes) aes?.Dispose();
        if (_ownsStream)
        {
            _reader.Dispose();
            _stream.Dispose();
        }
    }
}

public class PkgEntry
{
    public string Name { get; set; } = "";
    public uint NameOffset { get; set; }
    public uint NameSize { get; set; }
    public uint DataOffset { get; set; }
    public uint DataSize { get; set; }
    public byte ContentType { get; set; }
    public byte FileType { get; set; }
    public bool IsDirectory { get; set; }
}
