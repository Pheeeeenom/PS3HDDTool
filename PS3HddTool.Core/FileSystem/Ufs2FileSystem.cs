using System.Buffers.Binary;
using System.Text;
using PS3HddTool.Core.Disk;

namespace PS3HddTool.Core.FileSystem;

/// <summary>
/// UFS2 reader for big-endian PS3 and little-endian PS4 partitions.
/// 
/// Key structures:
///   - Superblock at byte offset 65536 (0x10000) from partition start
///   - Cylinder groups contain inode tables and block bitmaps
///   - Inodes describe files and directories
///   - Directory entries are variable-length records within directory data blocks
/// </summary>
public class Ufs2FileSystem
{
    private readonly IDiskSource _disk;
    private readonly long _partitionOffsetBytes;
    public Ufs2Superblock? Superblock { get; private set; }
    public IDiskSource DiskSource => _disk;
    public long PartitionOffsetBytes => _partitionOffsetBytes;
    private UfsByteOrder Reader => new(Superblock?.IsLittleEndian == true);

    public Ufs2FileSystem(IDiskSource disk, long partitionStartSector)
    {
        if (partitionStartSector < 0 || partitionStartSector > disk.SectorCount)
            throw new ArgumentOutOfRangeException(nameof(partitionStartSector));
        _disk = disk;
        _partitionOffsetBytes = checked(partitionStartSector * 512);
    }

    /// <summary>
    /// Read and parse the UFS2 superblock.
    /// </summary>
    public bool Mount()
    {
        byte[] sbData = _disk.ReadBytes(_partitionOffsetBytes + 65536, 8192);
        RawSuperblockData = sbData;
        Superblock = Ufs2Superblock.Parse(sbData);
        if (!Superblock.IsValid) return false;
        Superblock.ValidateGeometry(_disk.TotalSize - _partitionOffsetBytes);
        return true;
    }

    /// <summary>Raw superblock bytes for debugging.</summary>
    public byte[]? RawSuperblockData { get; private set; }

    /// <summary>
    /// Read an inode by number.
    /// </summary>
    public Ufs2Inode ReadInode(long inodeNumber)
    {
        if (Superblock == null) throw new InvalidOperationException("Filesystem not mounted.");

        if (inodeNumber < 0 || inodeNumber >= checked(Superblock.InodesPerGroup * Superblock.CylinderGroups))
            throw new InvalidDataException("Inode number is outside the filesystem.");
        long inodesPerGroup = Superblock.InodesPerGroup;
        long group = inodeNumber / inodesPerGroup;
        long indexInGroup = inodeNumber % inodesPerGroup;

        long cgOffset = checked(_partitionOffsetBytes + (group * Superblock.FragsPerGroup * Superblock.FragmentSize));
        long inodeTableOffset = checked(cgOffset + (Superblock.InodeBlockOffset * Superblock.FragmentSize));
        long inodeOffset = checked(inodeTableOffset + (indexInGroup * Superblock.InodeSize));
        if (inodeOffset > _disk.TotalSize - Superblock.InodeSize)
            throw new InvalidDataException("Inode extends past the source.");

        byte[] inodeData = _disk.ReadBytes(inodeOffset, (int)Superblock.InodeSize);
        return Ufs2Inode.Parse(inodeData, inodeNumber, Superblock.IsLittleEndian);
    }


    /// <summary>
    /// List the contents of a directory given its inode.
    /// </summary>
    public List<Ufs2DirectoryEntry> ReadDirectory(Ufs2Inode dirInode)
    {
        if (Superblock == null) throw new InvalidOperationException("Filesystem not mounted.");
        if (dirInode.FileType != Ufs2FileType.Directory)
            throw new ArgumentException("Inode is not a directory.");

        var entries = new List<Ufs2DirectoryEntry>();
        if (dirInode.Size < 0 || dirInode.Size > 64 * 1024 * 1024)
            throw new InvalidDataException("Directory is too large or has an invalid size.");
        byte[] dirData = ReadInodeData(dirInode);

        int offset = 0;
        while (offset < dirData.Length)
        {
            if (offset + 8 > dirData.Length) throw new InvalidDataException("Truncated directory record.");

            uint ino = Reader.ReadUInt32(dirData.AsSpan(offset));
            ushort recLen = Reader.ReadUInt16(dirData.AsSpan(offset + 4));
            byte fileType = dirData[offset + 6];
            byte nameLen = dirData[offset + 7];

            if (recLen < 8 || recLen % 4 != 0 || recLen > dirData.Length - offset || nameLen > recLen - 8)
                throw new InvalidDataException("Invalid UFS2 directory record.");

            if (ino != 0 && nameLen > 0 && offset + 8 + nameLen <= dirData.Length)
            {
                string name = Encoding.UTF8.GetString(dirData, offset + 8, nameLen);
                if (name.IndexOfAny(new[] { '/', '\0' }) >= 0)
                    throw new InvalidDataException("Invalid UFS2 filename.");
                entries.Add(new Ufs2DirectoryEntry
                {
                    InodeNumber = ino,
                    Name = name,
                    FileType = (Ufs2DirEntryType)fileType,
                    RecordLength = recLen
                });
            }

            offset += recLen;
        }

        return entries;
    }

    /// <summary>
    /// Read all data blocks for an inode, returning the file's content.
    /// </summary>
    public byte[] ReadInodeData(Ufs2Inode inode)
    {
        if (inode.Size < 0 || inode.Size > Array.MaxLength)
            throw new InvalidDataException("File is too large for an in-memory read; use streaming extraction.");
        using var output = new MemoryStream();
        ExtractInodeToStream(inode, output);
        return output.ToArray();
    }

    /// <summary>Stream direct and indirect blocks with bounded reads, preserving sparse holes.</summary>
    public void ExtractInodeToStream(Ufs2Inode inode, Stream output, Action<long>? progress = null)
    {
        if (Superblock == null) throw new InvalidOperationException("Filesystem not mounted.");
        if (inode.Size < 0) throw new InvalidDataException("Negative inode size.");
        if (inode.Size == 0) return;
        int blockSize = checked((int)Superblock.BlockSize);
        long fragmentsPerBlock = blockSize / Superblock.FragmentSize;
        const int maxReadBytes = 4 * 1024 * 1024;
        using var blocks = EnumerateBlocks(inode).GetEnumerator();
        bool hasBlock = blocks.MoveNext();
        long written = 0;
        byte[] zeros = new byte[blockSize];
        while (hasBlock && written < inode.Size)
        {
            long first = blocks.Current;
            if (first < 0) throw new InvalidDataException("Negative UFS2 block pointer.");
            int run = 1;
            hasBlock = blocks.MoveNext();
            if (first != 0)
            {
                while (hasBlock && run < maxReadBytes / blockSize &&
                       blocks.Current == first + run * fragmentsPerBlock)
                {
                    run++;
                    hasBlock = blocks.MoveNext();
                }
            }
            int count = (int)Math.Min((long)run * blockSize, inode.Size - written);
            byte[] data = first == 0 ? zeros : ReadFragmentRange(first, count);
            output.Write(data, 0, count);
            written += count;
            progress?.Invoke(written);
        }
        if (written != inode.Size) throw new InvalidDataException("Inode size exceeds its block address capacity.");
    }

    private IEnumerable<long> EnumerateBlocks(Ufs2Inode inode)
    {
        long blockSize = Superblock!.BlockSize;
        long remaining = inode.Size / blockSize + (inode.Size % blockSize == 0 ? 0 : 1);
        foreach (long block in inode.DirectBlocks)
        {
            if (remaining-- <= 0) yield break;
            yield return block;
        }
        long capacity = 1;
        long[] roots = { inode.IndirectBlock, inode.DoubleIndirectBlock, inode.TripleIndirectBlock };
        for (int level = 1; level <= 3 && remaining > 0; level++)
        {
            capacity = checked(capacity * (blockSize / 8));
            long count = Math.Min(remaining, capacity);
            foreach (long block in EnumerateIndirect(roots[level - 1], level, count)) yield return block;
            remaining -= count;
        }
        if (remaining > 0) throw new InvalidDataException("Inode size exceeds its block address capacity.");
    }

    private IEnumerable<long> EnumerateIndirect(long address, int level, long count)
    {
        if (address == 0)
        {
            for (long i = 0; i < count; i++) yield return 0;
            yield break;
        }
        byte[] pointers = ReadFragmentRange(address, (int)Superblock!.BlockSize);
        long childCapacity = 1;
        for (int i = 1; i < level; i++) childCapacity *= Superblock.BlockSize / 8;
        for (int i = 0; count > 0; i++)
        {
            long pointer = Reader.ReadInt64(pointers.AsSpan(i * 8));
            if (pointer < 0) throw new InvalidDataException("Negative UFS2 block pointer.");
            long take = Math.Min(count, childCapacity);
            if (level == 1) yield return pointer;
            else foreach (long block in EnumerateIndirect(pointer, level - 1, take)) yield return block;
            count -= take;
        }
    }

    private byte[] ReadFragmentRange(long fragment, int count)
    {
        if (fragment < 0 || fragment > (long.MaxValue - _partitionOffsetBytes) / Superblock!.FragmentSize)
            throw new InvalidDataException("Invalid UFS2 fragment address.");
        long offset = _partitionOffsetBytes + fragment * Superblock.FragmentSize;
        if (offset > _disk.TotalSize - count)
            throw new InvalidDataException("UFS2 block extends past the source.");
        return _disk.ReadBytes(offset, count);
    }

    /// <summary>
    /// Navigate to a path and return the inode, starting from root inode (2).
    /// </summary>
    public Ufs2Inode? ResolvePath(string path)
    {
        if (string.IsNullOrEmpty(path) || path == "/")
            return ReadInode(2); // Root inode is always 2 in UFS2

        string[] parts = path.Trim('/').Split('/');
        Ufs2Inode current = ReadInode(2);

        foreach (string part in parts)
        {
            if (current.FileType != Ufs2FileType.Directory)
                return null;

            var entries = ReadDirectory(current);
            var match = entries.FirstOrDefault(e => e.Name == part);
            if (match == null) return null;

            current = ReadInode(match.InodeNumber);
        }

        return current;
    }

    /// <summary>
    /// Extract a file to disk.
    /// </summary>
    public void ExtractFile(Ufs2Inode inode, string outputPath, Action<long>? progress = null)
    {
        RejectLink(outputPath);
        string? dir = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        using var fs = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
        ExtractInodeToStream(inode, fs, progress);
    }

    /// <summary>
    /// Recursively extract a directory tree.
    /// </summary>
    public void ExtractDirectory(Ufs2Inode dirInode, string outputDir, IProgress<string>? progress = null)
        => ExtractDirectory(dirInode, outputDir, progress, new HashSet<long>());

    private void ExtractDirectory(Ufs2Inode dirInode, string outputDir, IProgress<string>? progress, HashSet<long> ancestors)
    {
        if (ancestors.Count >= 256 || !ancestors.Add(dirInode.InodeNumber))
            throw new InvalidDataException("Directory cycle or excessive nesting encountered.");
        RejectLink(outputDir);
        Directory.CreateDirectory(outputDir);
        try
        {
            foreach (var entry in ReadDirectory(dirInode))
            {
                if (entry.Name == "." || entry.Name == "..") continue;
                string outputPath = GetExtractionPath(outputDir, entry.Name);
                var inode = ReadInode(entry.InodeNumber);
                progress?.Report(outputPath);
                if (inode.FileType == Ufs2FileType.Directory)
                    ExtractDirectory(inode, outputPath, progress, ancestors);
                else if (inode.FileType == Ufs2FileType.RegularFile)
                    ExtractFile(inode, outputPath);
            }
        }
        finally { ancestors.Remove(dirInode.InodeNumber); }
    }

    public static string GetExtractionPath(string directory, string name)
    {
        if (string.IsNullOrWhiteSpace(name) || name is "." or ".." ||
            name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 || name.Contains('/') || name.Contains('\\') ||
            name.EndsWith('.') || name.EndsWith(' '))
            throw new InvalidDataException($"Cannot extract filename '{name}' on this host.");
        return Path.Combine(directory, name);
    }

    private static void RejectLink(string path)
    {
        try
        {
            if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
                throw new IOException("Extraction cannot overwrite or follow a destination symbolic link.");
        }
        catch (FileNotFoundException) { }
        catch (DirectoryNotFoundException) { }
    }
}

#region UFS2 Structures

/// <summary>
/// UFS2 Superblock — contains filesystem geometry and metadata.
/// </summary>
public class Ufs2Superblock
{
    public const uint Ufs2Magic = 0x19540119;

    // Key fields
    public uint Magic { get; set; }
    public long BlockSize { get; set; }        // fs_bsize: fragment/block size
    public long FragmentSize { get; set; }      // fs_fsize
    public long FragsPerGroup { get; set; }     // fs_fpg
    public long InodesPerGroup { get; set; }    // fs_ipg
    public long InodeBlockOffset { get; set; }  // fs_iblkno: offset of inode blocks in CG
    public long InodeSize { get; set; }         // fs_isize (typically 256 for UFS2)
    public long TotalFragments { get; set; }    // fs_size
    public long TotalDataFragments { get; set; }// fs_dsize
    public int CylinderGroups { get; set; }     // fs_ncg
    public string VolumeName { get; set; } = "";

    public bool IsValid => Magic == Ufs2Magic;
    public bool IsLittleEndian { get; private set; }

    // Free space summary (fs_cstotal at offset 0x3F0)
    public long FreeBlocks { get; set; }     // cs_nbfree
    public long FreeInodes { get; set; }     // cs_nifree
    public long FreeFragments { get; set; }  // cs_nffree
    public long Directories { get; set; }    // cs_ndir

    /// <summary>Free space in bytes, computed from free blocks + free fragments.</summary>
    public long FreeSpaceBytes => (FreeBlocks * BlockSize) + (FreeFragments * FragmentSize);

    public void ValidateGeometry(long availableBytes)
    {
        static bool PowerOfTwo(long value) => value > 0 && (value & (value - 1)) == 0;
        if (!IsValid || !PowerOfTwo(BlockSize) || BlockSize is < 4096 or > 65536 ||
            !PowerOfTwo(FragmentSize) || FragmentSize < 512 || FragmentSize > BlockSize ||
            BlockSize / FragmentSize > 8 || InodesPerGroup <= 0 || FragsPerGroup <= 0 ||
            CylinderGroups <= 0 || InodeBlockOffset <= 0 || InodeBlockOffset >= FragsPerGroup ||
            InodesPerGroup > FragsPerGroup * FragmentSize / 256 ||
            (long)(CylinderGroups - 1) > availableBytes / FragmentSize / FragsPerGroup)
            throw new InvalidDataException("Invalid UFS2 filesystem geometry.");
        if (IsLittleEndian && (TotalFragments <= 0 || TotalFragments > availableBytes / FragmentSize))
            throw new InvalidDataException("UFS2 filesystem extends past the partition.");
    }

    public static Ufs2Superblock Parse(byte[] data)
    {
        var sb = new Ufs2Superblock();
        if (data.Length < 0x560) return sb;
        sb.IsLittleEndian = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0x55C)) == Ufs2Magic;
        var reader = new UfsByteOrder(sb.IsLittleEndian);

        // The magic number is at offset 0x55C (1372) in the superblock
        if (data.Length >= 0x560)
        {
            sb.Magic = reader.ReadUInt32(data.AsSpan(0x55C));
        }

        if (!sb.IsValid) return sb;

        // Parse key fields (offsets from FreeBSD sys/ufs/ffs/fs.h)
        // PS3 is big-endian; PS4 is little-endian. Preserve the existing PS3
        // size/name field layout while using the standard UFS2 offsets for PS4.
        sb.BlockSize = reader.ReadInt32(data.AsSpan(0x30));       // fs_bsize
        sb.FragmentSize = reader.ReadInt32(data.AsSpan(0x34));    // fs_fsize
        sb.FragsPerGroup = reader.ReadInt32(data.AsSpan(0xBC));   // fs_fpg (NOT 0x74!)
        sb.InodesPerGroup = reader.ReadInt32(data.AsSpan(0xB8));  // fs_ipg
        sb.InodeBlockOffset = reader.ReadInt32(data.AsSpan(0x10));// fs_iblkno
        sb.TotalFragments = reader.ReadInt64(data.AsSpan(sb.IsLittleEndian ? 0x438 : 0x218)); // fs_size (UFS2 64-bit)
        sb.TotalDataFragments = reader.ReadInt64(data.AsSpan(sb.IsLittleEndian ? 0x440 : 0x220)); // fs_dsize
        sb.CylinderGroups = reader.ReadInt32(data.AsSpan(0x2C));  // fs_ncg (NOT 0xBC!)
        sb.InodeSize = 256; // UFS2 always uses 256-byte inodes

        // fs_cstotal at 0x3F0: int64 fields in the filesystem's byte order.
        if (data.Length >= 0x418)
        {
            sb.Directories = reader.ReadInt64(data.AsSpan(0x3F0));    // cs_ndir
            sb.FreeBlocks = reader.ReadInt64(data.AsSpan(0x3F8));     // cs_nbfree
            sb.FreeInodes = reader.ReadInt64(data.AsSpan(0x400));     // cs_nifree
            sb.FreeFragments = reader.ReadInt64(data.AsSpan(0x408));  // cs_nffree
        }

        // Volume name, 32 bytes max.
        if (data.Length >= 0x4A0)
        {
            int nameOffset = sb.IsLittleEndian ? 0x2A8 : 0x480;
            int nameEnd = Array.IndexOf(data, (byte)0, nameOffset, 32);
            if (nameEnd < 0) nameEnd = nameOffset + 32;
            sb.VolumeName = Encoding.UTF8.GetString(data, nameOffset, nameEnd - nameOffset).TrimEnd('\0');
        }

        return sb;
    }
}

/// <summary>
/// UFS2 Inode — describes a file or directory.
/// </summary>
public class Ufs2Inode
{
    public long InodeNumber { get; set; }
    public byte[]? RawBytes { get; set; }
    public Ufs2FileType FileType { get; set; }
    public ushort Mode { get; set; }
    public short LinkCount { get; set; }
    public uint Uid { get; set; }
    public uint Gid { get; set; }
    public long Size { get; set; }
    public long AccessTime { get; set; }
    public long ModifyTime { get; set; }
    public long ChangeTime { get; set; }
    public long CreateTime { get; set; }
    public long[] DirectBlocks { get; set; } = new long[12];
    public long IndirectBlock { get; set; }
    public long DoubleIndirectBlock { get; set; }
    public long TripleIndirectBlock { get; set; }
    public uint Flags { get; set; }
    public long Blocks { get; set; }

    public DateTime ModifyDateTime => SafeFromUnix(ModifyTime);
    public DateTime CreateDateTime => SafeFromUnix(CreateTime);

    private static DateTime SafeFromUnix(long seconds)
    {
        // Valid Unix timestamp range for DateTimeOffset.FromUnixTimeSeconds
        if (seconds >= -62135596800L && seconds <= 253402300799L)
            return DateTimeOffset.FromUnixTimeSeconds(seconds).DateTime;
        return DateTime.MinValue;
    }

    public string ModeString
    {
        get
        {
            char type = FileType switch
            {
                Ufs2FileType.Directory => 'd',
                Ufs2FileType.SymbolicLink => 'l',
                Ufs2FileType.RegularFile => '-',
                _ => '?'
            };
            return $"{type}{FormatPermissions(Mode)}";
        }
    }

    private static string FormatPermissions(ushort mode)
    {
        char[] perms = new char[9];
        perms[0] = (mode & 0x100) != 0 ? 'r' : '-';
        perms[1] = (mode & 0x080) != 0 ? 'w' : '-';
        perms[2] = (mode & 0x040) != 0 ? 'x' : '-';
        perms[3] = (mode & 0x020) != 0 ? 'r' : '-';
        perms[4] = (mode & 0x010) != 0 ? 'w' : '-';
        perms[5] = (mode & 0x008) != 0 ? 'x' : '-';
        perms[6] = (mode & 0x004) != 0 ? 'r' : '-';
        perms[7] = (mode & 0x002) != 0 ? 'w' : '-';
        perms[8] = (mode & 0x001) != 0 ? 'x' : '-';
        return new string(perms);
    }

    public static Ufs2Inode Parse(byte[] data, long inodeNumber, bool littleEndian = false)
    {
        if (data.Length < 256) throw new InvalidDataException("Truncated UFS2 inode.");
        var reader = new UfsByteOrder(littleEndian);
        var inode = new Ufs2Inode { InodeNumber = inodeNumber, RawBytes = (byte[])data.Clone() };

        // FreeBSD UFS2 dinode layout (256 bytes, from sys/ufs/ufs/dinode.h):
        // 0x00: u16 di_mode
        // 0x02: i16 di_nlink
        // 0x04: u32 di_uid
        // 0x08: u32 di_gid
        // 0x0C: u32 di_blksize
        // 0x10: i64 di_size
        // 0x18: i64 di_blocks
        // PS3 packs timestamps non-interleaved (verified from native inode hex dumps):
        // 0x20: i64 di_atime
        // 0x28: i64 di_mtime
        // 0x30: i64 di_ctime
        // 0x38: i64 di_birthtime
        // 0x40: i32 di_atimensec, 0x44: i32 di_mtimensec, 0x48: i32 di_ctimensec, 0x4C: i32 di_birthnsec
        // 0x50: u32 di_gen
        // 0x54: u32 di_kernflags
        // 0x58: u32 di_flags
        // 0x5C: u32 di_extsize
        // 0x60: i64 di_extb[2]     (2 x 8-byte extended attribute block ptrs)
        // 0x70: i64 di_db[12]      (12 x 8-byte direct block pointers)
        // 0xD0: i64 di_ib[3]       (3 x 8-byte indirect block pointers)
        // 0xE8: i64 di_modrev
        // 0xF0: u32 di_freelink
        // ... padding to 256 bytes

        ushort mode = reader.ReadUInt16(data.AsSpan(0x00));
        inode.Mode = mode;
        inode.FileType = (mode & 0xF000) switch
        {
            0x4000 => Ufs2FileType.Directory,
            0x8000 => Ufs2FileType.RegularFile,
            0xA000 => Ufs2FileType.SymbolicLink,
            0x6000 => Ufs2FileType.BlockDevice,
            0x2000 => Ufs2FileType.CharDevice,
            0x1000 => Ufs2FileType.Fifo,
            0xC000 => Ufs2FileType.Socket,
            _ => Ufs2FileType.Unknown
        };

        inode.LinkCount = reader.ReadInt16(data.AsSpan(0x02));
        inode.Uid = reader.ReadUInt32(data.AsSpan(0x04));
        inode.Gid = reader.ReadUInt32(data.AsSpan(0x08));
        inode.Size = reader.ReadInt64(data.AsSpan(0x10));
        inode.Blocks = reader.ReadInt64(data.AsSpan(0x18));

        // PS3 UFS2 inode timestamp layout (non-interleaved):
        //   0x20: di_atime (8B), 0x28: di_mtime (8B), 0x30: di_ctime (8B), 0x38: di_birthtime (8B)
        //   0x40-0x4F: nanosecond fields (4 x 4B)
        // Verified by comparing PS3-native inode hex dumps.
        inode.AccessTime = reader.ReadInt64(data.AsSpan(0x20));
        inode.ModifyTime = reader.ReadInt64(data.AsSpan(0x28));
        inode.ChangeTime = reader.ReadInt64(data.AsSpan(0x30));
        inode.CreateTime = reader.ReadInt64(data.AsSpan(0x38));

        inode.Flags = reader.ReadUInt32(data.AsSpan(0x58));

        // Direct block pointers at offset 0x70 (12 x 8 bytes = 96 bytes)
        for (int i = 0; i < 12; i++)
            inode.DirectBlocks[i] = reader.ReadInt64(data.AsSpan(0x70 + i * 8));

        // Indirect block pointers at offset 0xD0
        inode.IndirectBlock = reader.ReadInt64(data.AsSpan(0xD0));
        inode.DoubleIndirectBlock = reader.ReadInt64(data.AsSpan(0xD8));
        inode.TripleIndirectBlock = reader.ReadInt64(data.AsSpan(0xE0));

        return inode;
    }
}

public enum Ufs2FileType
{
    Unknown,
    Fifo,
    CharDevice,
    Directory,
    BlockDevice,
    RegularFile,
    SymbolicLink,
    Socket
}

/// <summary>
/// UFS2 Directory Entry — links a name to an inode number.
/// </summary>
public class Ufs2DirectoryEntry
{
    public uint InodeNumber { get; set; }
    public string Name { get; set; } = "";
    public Ufs2DirEntryType FileType { get; set; }
    public ushort RecordLength { get; set; }
}

public enum Ufs2DirEntryType : byte
{
    Unknown = 0,
    RegularFile = 1,
    Directory = 2,
    CharDevice = 3,
    BlockDevice = 4,
    Fifo = 5,
    Socket = 6,
    SymbolicLink = 7,
    Whiteout = 8
}

#endregion
