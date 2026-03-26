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
/// PKG encryption mode.
/// </summary>
public enum PkgCryptoMode
{
    /// <summary>Retail/finalized: AES-128-CTR with PS3 constant key + FileKey as IV.</summary>
    AesCtr,
    /// <summary>Debug/non-finalized: SHA1-based XOR stream cipher seeded from header digest.</summary>
    Sha1Xor
}

/// <summary>
/// Reads and extracts PS3 NPDRM PKG files.
/// Supports both retail (finalized, AES-CTR) and debug/homebrew (SHA1-XOR) packages.
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

    // Pre-allocated per-thread AES encryptors for parallel AES-CTR decrypt
    private Aes[]? _threadAes;
    private ICryptoTransform[]? _threadEncryptors;
    private int _threadCount;

    // SHA1-XOR key (64 bytes), built from QaDigest for debug PKGs
    private byte[]? _sha1BaseKey;

    // Header fields
    public byte[] Magic { get; private set; } = new byte[4];
    public byte PkgRevision { get; private set; }
    public bool IsFinalized => (PkgRevision & 0x80) != 0;
    public PkgCryptoMode CryptoMode { get; private set; }
    public byte PkgType { get; private set; }
    public uint MetadataOffset { get; private set; }
    public uint MetadataCount { get; private set; }
    public uint HeaderSize { get; private set; }
    public uint DataOffset { get; private set; }
    public ulong DataSize { get; private set; }
    public string ContentId { get; private set; } = "";
    public byte[] FileKey { get; private set; } = new byte[16];
    /// <summary>Header bytes at offset 0x60 — used as seed for debug SHA1-XOR decryption.</summary>
    public byte[] QaDigest { get; private set; } = new byte[16];
    public uint FileCount { get; private set; }
    public ulong TotalSize { get; private set; }

    public List<PkgEntry> Entries { get; private set; } = new();

    // PARAM.SFO data
    public string TitleId { get; private set; } = "";
    public string Title { get; private set; } = "";

    public Ps3PkgReader(string filePath, Action<string>? log = null)
    {
        _stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        _reader = new BinaryReader(_stream);
        _ownsStream = true;
        _log = log;
        ParseHeader();
        InitCrypto();
        ParseFileTable();
        FindTitleId();
    }

    public Ps3PkgReader(Stream stream)
    {
        _stream = stream;
        _reader = new BinaryReader(_stream);
        _ownsStream = false;
        ParseHeader();
        InitCrypto();
        ParseFileTable();
        FindTitleId();
    }

    private void InitCrypto()
    {
        if (CryptoMode == PkgCryptoMode.AesCtr)
        {
            _aes = Aes.Create();
            _aes.Key = PS3_AES_KEY;
            _aes.Mode = CipherMode.ECB;
            _aes.Padding = PaddingMode.None;
            _aesEncryptor = _aes.CreateEncryptor();

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
        else // Sha1Xor
        {
            // Build 64-byte key from digest at 0x60
            _sha1BaseKey = new byte[0x40];
            Buffer.BlockCopy(QaDigest, 0, _sha1BaseKey, 0x00, 8);
            Buffer.BlockCopy(QaDigest, 0, _sha1BaseKey, 0x08, 8);
            Buffer.BlockCopy(QaDigest, 8, _sha1BaseKey, 0x10, 8);
            Buffer.BlockCopy(QaDigest, 8, _sha1BaseKey, 0x18, 8);
            // bytes 0x20-0x3F remain zero

            _threadCount = Environment.ProcessorCount;
        }
    }

    private void ParseHeader()
    {
        _stream.Seek(0, SeekOrigin.Begin);
        Magic = _reader.ReadBytes(4);

        if (Magic[0] != 0x7F || Magic[1] != 0x50 || Magic[2] != 0x4B || Magic[3] != 0x47)
            throw new InvalidDataException("Not a valid PS3 PKG file (bad magic).");

        PkgRevision = _reader.ReadByte();
        CryptoMode = IsFinalized ? PkgCryptoMode.AesCtr : PkgCryptoMode.Sha1Xor;
        _log?.Invoke($"PKG revision: 0x{PkgRevision:X2} ({(IsFinalized ? "retail/finalized \u2192 AES-CTR" : "debug/homebrew \u2192 SHA1-XOR")})");

        _reader.ReadBytes(2); // skip

        PkgType = _reader.ReadByte();
        if (PkgType != 0x01)
            throw new InvalidDataException($"Not a PS3 PKG (type=0x{PkgType:X2}). Only PS3 (0x01) is supported.");

        _stream.Seek(0x08, SeekOrigin.Begin);
        MetadataOffset = ReadU32BE();
        MetadataCount = ReadU32BE();
        HeaderSize = ReadU32BE();
        FileCount = ReadU32BE();

        TotalSize = ReadU64BE();

        _stream.Seek(0x20, SeekOrigin.Begin);
        DataOffset = (uint)ReadU64BE();
        DataSize = ReadU64BE();

        _stream.Seek(0x30, SeekOrigin.Begin);
        byte[] contentIdBytes = _reader.ReadBytes(48);
        ContentId = Encoding.ASCII.GetString(contentIdBytes).TrimEnd('\0');

        _stream.Seek(0x60, SeekOrigin.Begin);
        QaDigest = _reader.ReadBytes(16);

        _stream.Seek(0x70, SeekOrigin.Begin);
        FileKey = _reader.ReadBytes(16);
    }

    private void ParseFileTable()
    {
        Entries.Clear();

        int tableSize = (int)(FileCount * 32);
        byte[] rawTable = new byte[tableSize];

        _stream.Seek(DataOffset, SeekOrigin.Begin);
        _stream.Read(rawTable, 0, tableSize);

        byte[] decryptedTable = DecryptData(rawTable, 0, tableSize);

        // Sanity check first entry
        if (FileCount > 0)
        {
            uint checkNameSize = BinaryPrimitives.ReadUInt32BigEndian(decryptedTable.AsSpan(4));
            if (checkNameSize > 4096 || checkNameSize == 0)
            {
                _log?.Invoke($"  File table sanity check failed (nameSize={checkNameSize}). Trying opposite crypto mode...");
                CryptoMode = CryptoMode == PkgCryptoMode.AesCtr ? PkgCryptoMode.Sha1Xor : PkgCryptoMode.AesCtr;
                DisposeCrypto();
                InitCrypto();
                decryptedTable = DecryptData(rawTable, 0, tableSize);
                checkNameSize = BinaryPrimitives.ReadUInt32BigEndian(decryptedTable.AsSpan(4));
                if (checkNameSize > 4096 || checkNameSize == 0)
                    throw new InvalidDataException($"PKG file table is corrupt or uses an unsupported format (nameSize={checkNameSize} after both crypto modes).");
                _log?.Invoke($"  Opposite mode succeeded: {CryptoMode} (nameSize={checkNameSize}).");
            }
        }

        for (int i = 0; i < (int)FileCount; i++)
        {
            int off = i * 32;

            uint nameOffset = BinaryPrimitives.ReadUInt32BigEndian(decryptedTable.AsSpan(off));
            uint nameSize = BinaryPrimitives.ReadUInt32BigEndian(decryptedTable.AsSpan(off + 4));
            ulong dataOffset = BinaryPrimitives.ReadUInt64BigEndian(decryptedTable.AsSpan(off + 8));
            ulong dataSize = BinaryPrimitives.ReadUInt64BigEndian(decryptedTable.AsSpan(off + 16));
            byte contentType = decryptedTable[off + 24];
            byte fileType = decryptedTable[off + 27];

            if (nameSize > 4096 || nameSize == 0)
            {
                _log?.Invoke($"  Skipping entry {i}: invalid nameSize={nameSize}");
                continue;
            }
            if (nameOffset + nameSize > (ulong)_stream.Length)
            {
                _log?.Invoke($"  Skipping entry {i}: nameOffset 0x{nameOffset:X} + nameSize {nameSize} exceeds file size");
                continue;
            }

            byte[] encName = new byte[nameSize];
            _stream.Seek(DataOffset + nameOffset, SeekOrigin.Begin);
            _stream.Read(encName, 0, (int)nameSize);
            byte[] decName = DecryptData(encName, (long)nameOffset, (int)nameSize);
            string name = Encoding.ASCII.GetString(decName, 0, (int)nameSize).TrimEnd('\0');

            bool isDir = ((fileType & 0x0F) == 0x04 && dataSize == 0);

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
        if (ContentId.Length >= 16)
        {
            int dash1 = ContentId.IndexOf('-');
            if (dash1 >= 0 && dash1 + 10 < ContentId.Length)
            {
                int dash2 = ContentId.IndexOf('-', dash1 + 1);
                if (dash2 > dash1)
                    TitleId = ContentId.Substring(dash1 + 1, dash2 - dash1 - 1);
            }
        }

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
                        if (!string.IsNullOrEmpty(sfo.TitleId)) TitleId = sfo.TitleId;
                        if (!string.IsNullOrEmpty(sfo.Title)) Title = sfo.Title;
                    }
                }
                catch { }
                break;
            }
        }
    }

    // ========================================================================
    // Decryption dispatch
    // ========================================================================

    private byte[] DecryptData(byte[] encrypted, long dataRelativeOffset, int size)
    {
        return CryptoMode switch
        {
            PkgCryptoMode.AesCtr => DecryptAesCtr(encrypted, dataRelativeOffset, size),
            PkgCryptoMode.Sha1Xor => DecryptSha1Xor(encrypted, dataRelativeOffset, size),
            _ => encrypted
        };
    }

    private byte[] DecryptDataParallelTimed(byte[] encrypted, long dataRelativeOffset, int size,
        out long counterTicks, out long aesTicks, out long xorTicks)
    {
        counterTicks = 0; aesTicks = 0; xorTicks = 0;

        if (CryptoMode == PkgCryptoMode.Sha1Xor)
        {
            long t0 = System.Diagnostics.Stopwatch.GetTimestamp();
            byte[] result = DecryptSha1XorParallel(encrypted, dataRelativeOffset, size);
            long t1 = System.Diagnostics.Stopwatch.GetTimestamp();
            aesTicks = t1 - t0;
            return result;
        }

        return DecryptAesCtrParallelTimed(encrypted, dataRelativeOffset, size,
            out counterTicks, out aesTicks, out xorTicks);
    }

    // ========================================================================
    // AES-CTR (retail/finalized)
    // ========================================================================

    private byte[] DecryptAesCtr(byte[] encrypted, long dataRelativeOffset, int size)
    {
        int alignedSize = (size + 15) & ~15;
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

    private byte[] DecryptAesCtrParallelTimed(byte[] encrypted, long dataRelativeOffset, int size,
        out long counterTicks, out long aesTicks, out long xorTicks)
    {
        int alignedSize = (size + 15) & ~15;
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

            byte[] counterSeg = new byte[segmentBytes];
            FillCounterBlocks(FileKey, startBlock + blockStart, counterSeg, 0, segmentBlocks);

            byte[] keystreamSeg = _threadEncryptors![thread].TransformFinalBlock(counterSeg, 0, counterSeg.Length);

            int xorEnd = Math.Min(byteOffset + segmentBytes, size);
            for (int i = byteOffset; i < xorEnd; i++)
                result[i] = (byte)(encrypted[i] ^ keystreamSeg[i - byteOffset]);
        });

        long ct1 = System.Diagnostics.Stopwatch.GetTimestamp();
        counterTicks = 0;
        aesTicks = ct1 - ct0;
        xorTicks = 0;

        if (result.Length != size)
        {
            byte[] trimmed = new byte[size];
            Buffer.BlockCopy(result, 0, trimmed, 0, size);
            return trimmed;
        }
        return result;
    }

    private static void FillCounterBlocks(byte[] fileKey, long startBlock, byte[] dest, int destOffset, int blockCount)
    {
        ulong keyHi = BinaryPrimitives.ReadUInt64BigEndian(fileKey.AsSpan(0));
        ulong keyLo = BinaryPrimitives.ReadUInt64BigEndian(fileKey.AsSpan(8));

        ulong lo = keyLo + (ulong)startBlock;
        ulong hi = keyHi;
        if (lo < keyLo) hi++;

        for (int b = 0; b < blockCount; b++)
        {
            int off = destOffset + b * 16;
            BinaryPrimitives.WriteUInt64BigEndian(dest.AsSpan(off), hi);
            BinaryPrimitives.WriteUInt64BigEndian(dest.AsSpan(off + 8), lo);
            lo++;
            if (lo == 0) hi++;
        }
    }

    // ========================================================================
    // SHA1-XOR (debug/non-finalized)
    // ========================================================================

    /// <summary>
    /// Decrypt data using SHA1-based XOR stream cipher (fail0verflow algorithm).
    /// The stream cipher uses a 64-byte key buffer with a BE u64 counter at offset 0x38.
    /// SHA1 of the key buffer produces 20 bytes; the first 16 are XORed with data.
    /// Counter increments every 16 bytes of stream position.
    /// </summary>
    private byte[] DecryptSha1Xor(byte[] encrypted, long dataRelativeOffset, int size)
    {
        byte[] result = new byte[size];

        byte[] key = new byte[0x40];
        Buffer.BlockCopy(_sha1BaseKey!, 0, key, 0, 0x40);

        long startBlock = dataRelativeOffset / 16;
        int startOffset = (int)(dataRelativeOffset % 16);

        BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(0x38), (ulong)startBlock);
        byte[] sha1Hash = SHA1.HashData(key);

        int posInBlock = startOffset;

        for (int i = 0; i < size; i++)
        {
            if (posInBlock >= 16)
            {
                posInBlock = 0;
                ulong counter = BinaryPrimitives.ReadUInt64BigEndian(key.AsSpan(0x38));
                counter++;
                BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(0x38), counter);
                sha1Hash = SHA1.HashData(key);
            }

            result[i] = (byte)(encrypted[i] ^ sha1Hash[posInBlock]);
            posInBlock++;
        }

        return result;
    }

    /// <summary>
    /// Parallel SHA1-XOR decrypt for large data.
    /// </summary>
    private byte[] DecryptSha1XorParallel(byte[] encrypted, long dataRelativeOffset, int size)
    {
        byte[] result = new byte[size];

        long startBlock = dataRelativeOffset / 16;
        int startOffset = (int)(dataRelativeOffset % 16);
        long totalBlocks = ((long)size + startOffset + 15) / 16;

        int threadCount = Math.Min(_threadCount, Math.Max(1, (int)(totalBlocks / 256)));
        if (threadCount < 2 || size < 65536)
            return DecryptSha1Xor(encrypted, dataRelativeOffset, size);

        long blocksPerThread = totalBlocks / threadCount;

        Parallel.For(0, threadCount, thread =>
        {
            long blockStart = thread * blocksPerThread;
            long blockEnd = (thread == threadCount - 1) ? totalBlocks : blockStart + blocksPerThread;

            long byteStart = blockStart * 16 - startOffset;
            if (byteStart < 0) byteStart = 0;
            long byteEnd = blockEnd * 16 - startOffset;
            if (byteEnd > size) byteEnd = size;
            if (byteStart >= byteEnd) return;

            byte[] key = new byte[0x40];
            Buffer.BlockCopy(_sha1BaseKey!, 0, key, 0, 0x40);

            long absStreamPos = dataRelativeOffset + byteStart;
            long currentBlock = absStreamPos / 16;
            int posInBlock = (int)(absStreamPos % 16);

            BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(0x38), (ulong)currentBlock);
            byte[] sha1Hash = SHA1.HashData(key);

            for (long i = byteStart; i < byteEnd; i++)
            {
                if (posInBlock >= 16)
                {
                    posInBlock = 0;
                    ulong counter = BinaryPrimitives.ReadUInt64BigEndian(key.AsSpan(0x38));
                    counter++;
                    BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(0x38), counter);
                    sha1Hash = SHA1.HashData(key);
                }

                result[i] = (byte)(encrypted[i] ^ sha1Hash[posInBlock]);
                posInBlock++;
            }
        });

        return result;
    }

    // ========================================================================
    // Extraction
    // ========================================================================

    public byte[] ExtractEntryToMemory(PkgEntry entry)
    {
        if (entry.IsDirectory || entry.DataSize == 0)
            return Array.Empty<byte>();

        if (entry.DataSize > int.MaxValue)
            throw new InvalidOperationException($"File too large for in-memory extraction ({entry.DataSize} bytes). Use ExtractEntryToFile instead.");

        byte[] encrypted = new byte[entry.DataSize];
        _stream.Seek((long)(DataOffset + entry.DataOffset), SeekOrigin.Begin);
        _stream.Read(encrypted, 0, (int)entry.DataSize);

        return DecryptData(encrypted, (long)entry.DataOffset, (int)entry.DataSize);
    }

    public void ExtractEntryToFile(PkgEntry entry, string outputPath)
    {
        if (entry.IsDirectory)
        {
            Directory.CreateDirectory(outputPath);
            return;
        }

        string? dir = Path.GetDirectoryName(outputPath);
        if (dir != null && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        const int CHUNK_SIZE = 1024 * 1024 * 64;
        long remaining = (long)entry.DataSize;
        long fileOffset = (long)entry.DataOffset;

        long readTicks = 0, aesTicks = 0, xorTicks = 0, waitTicks = 0;

        using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 1024);

        Task? pendingWrite = null;

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

            if (pendingWrite != null)
                pendingWrite.Wait();

            long t3 = System.Diagnostics.Stopwatch.GetTimestamp();

            var capturedData = decrypted;
            var capturedSize = toRead;
            var capturedStream = outStream;
            pendingWrite = Task.Run(() => capturedStream.Write(capturedData, 0, capturedSize));

            readTicks += t1 - t0;
            aesTicks += aeTicks;
            xorTicks += xrTicks;
            waitTicks += t3 - t2;

            fileOffset += toRead;
            remaining -= toRead;
        }

        long tw0 = System.Diagnostics.Stopwatch.GetTimestamp();
        pendingWrite?.Wait();
        long tw1 = System.Diagnostics.Stopwatch.GetTimestamp();
        waitTicks += tw1 - tw0;

        double freq = System.Diagnostics.Stopwatch.Frequency;
        if (entry.DataSize > 1024 * 1024)
        {
            _log?.Invoke($"  PKG TIMING [{entry.Name}]: read={readTicks * 1000 / freq:F0}ms, crypto={aesTicks * 1000 / freq:F0}ms, write_wait={waitTicks * 1000 / freq:F0}ms, size={entry.DataSize / (1024 * 1024)}MB, mode={CryptoMode}");
        }
    }

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
                Directory.CreateDirectory(fullPath);
            else
                ExtractEntryToFile(entry, fullPath);
        }

        return gameDir;
    }

    // ========================================================================
    // Helpers
    // ========================================================================

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

    private void DisposeCrypto()
    {
        _aesEncryptor?.Dispose(); _aesEncryptor = null;
        _aes?.Dispose(); _aes = null;
        if (_threadEncryptors != null)
            foreach (var enc in _threadEncryptors) enc?.Dispose();
        _threadEncryptors = null;
        if (_threadAes != null)
            foreach (var aes in _threadAes) aes?.Dispose();
        _threadAes = null;
        _sha1BaseKey = null;
    }

    public void Dispose()
    {
        DisposeCrypto();
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
    public ulong DataOffset { get; set; }
    public ulong DataSize { get; set; }
    public byte ContentType { get; set; }
    public byte FileType { get; set; }
    public bool IsDirectory { get; set; }
}
