using System.Buffers.Binary;
using System.Text;
using PS3HddTool.Core.Disk;

namespace PS3HddTool.Core.FileSystem;

/// <summary>
/// UFS2 (Unix File System 2) implementation for reading the PS3's GameOS partition.
/// The PS3 uses a FreeBSD-derived UFS2 filesystem.
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

    public Ufs2FileSystem(IDiskSource disk, long partitionStartSector)
    {
        _disk = disk;
        _partitionOffsetBytes = partitionStartSector * 512;
    }

    /// <summary>
    /// Read and parse the UFS2 superblock.
    /// </summary>
    public bool Mount()
    {
        byte[] sbData = _disk.ReadBytes(_partitionOffsetBytes + 65536, 8192);
        RawSuperblockData = sbData;
        Superblock = Ufs2Superblock.Parse(sbData);
        return Superblock.IsValid;
    }

    /// <summary>Raw superblock bytes for debugging.</summary>
    public byte[]? RawSuperblockData { get; private set; }

    /// <summary>
    /// Read an inode by number.
    /// </summary>
    public Ufs2Inode ReadInode(long inodeNumber)
    {
        if (Superblock == null) throw new InvalidOperationException("Filesystem not mounted.");

        long inodesPerGroup = Superblock.InodesPerGroup;
        long group = inodeNumber / inodesPerGroup;
        long indexInGroup = inodeNumber % inodesPerGroup;

        long cgOffset = _partitionOffsetBytes + (group * Superblock.FragsPerGroup * Superblock.FragmentSize);
        long inodeTableOffset = cgOffset + (Superblock.InodeBlockOffset * Superblock.FragmentSize);
        long inodeOffset = inodeTableOffset + (indexInGroup * Superblock.InodeSize);

        byte[] inodeData = _disk.ReadBytes(inodeOffset, (int)Superblock.InodeSize);
        return Ufs2Inode.Parse(inodeData, inodeNumber);
    }
    private int _inodeReadCount = 0;

    /// <summary>
    /// List the contents of a directory given its inode.
    /// </summary>
    public List<Ufs2DirectoryEntry> ReadDirectory(Ufs2Inode dirInode)
    {
        if (Superblock == null) throw new InvalidOperationException("Filesystem not mounted.");
        if (dirInode.FileType != Ufs2FileType.Directory)
            throw new ArgumentException("Inode is not a directory.");

        var entries = new List<Ufs2DirectoryEntry>();
        byte[] dirData = ReadInodeData(dirInode);

        int offset = 0;
        while (offset < dirData.Length)
        {
            if (offset + 8 > dirData.Length) break;

            uint ino = BinaryPrimitives.ReadUInt32BigEndian(dirData.AsSpan(offset));
            ushort recLen = BinaryPrimitives.ReadUInt16BigEndian(dirData.AsSpan(offset + 4));
            byte fileType = dirData[offset + 6];
            byte nameLen = dirData[offset + 7];

            if (recLen == 0) break;

            if (ino != 0 && nameLen > 0 && offset + 8 + nameLen <= dirData.Length)
            {
                string name = Encoding.ASCII.GetString(dirData, offset + 8, nameLen);
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
        if (Superblock == null) throw new InvalidOperationException("Filesystem not mounted.");

        long fileSize = inode.Size;
        if (fileSize == 0) return Array.Empty<byte>();

        long blockSize = Superblock.BlockSize;
        var data = new MemoryStream();

        // Read direct blocks (12 pointers)
        for (int i = 0; i < 12 && data.Length < fileSize; i++)
        {
            long blockAddr = inode.DirectBlocks[i];
            if (blockAddr == 0) break;

            byte[] blockData = ReadBlock(blockAddr);
            int toWrite = (int)Math.Min(blockData.Length, fileSize - data.Length);
            data.Write(blockData, 0, toWrite);
        }

        // Read single indirect blocks
        if (data.Length < fileSize && inode.IndirectBlock != 0)
        {
            ReadIndirectBlocks(inode.IndirectBlock, 1, data, fileSize);
        }

        // Read double indirect blocks
        if (data.Length < fileSize && inode.DoubleIndirectBlock != 0)
        {
            ReadIndirectBlocks(inode.DoubleIndirectBlock, 2, data, fileSize);
        }

        // Read triple indirect blocks
        if (data.Length < fileSize && inode.TripleIndirectBlock != 0)
        {
            ReadIndirectBlocks(inode.TripleIndirectBlock, 3, data, fileSize);
        }

        return data.ToArray();
    }

    /// <summary>
    /// Stream inode data directly to an output stream (for large file extraction).
    /// Reports progress via callback.
    /// </summary>
    public void ExtractInodeToStream(Ufs2Inode inode, Stream output, Action<long>? progress = null)
    {
        if (Superblock == null) throw new InvalidOperationException("Filesystem not mounted.");

        long fileSize = inode.Size;
        if (fileSize == 0) return;

        long written = 0;

        // Read direct blocks (12 pointers)
        for (int i = 0; i < 12 && written < fileSize; i++)
        {
            long blockAddr = inode.DirectBlocks[i];
            if (blockAddr == 0) break;

            byte[] blockData = ReadBlock(blockAddr);
            int toWrite = (int)Math.Min(blockData.Length, fileSize - written);
            output.Write(blockData, 0, toWrite);
            written += toWrite;
            progress?.Invoke(written);
        }

        // Read single indirect blocks
        if (written < fileSize && inode.IndirectBlock != 0)
            WriteIndirectBlocks(inode.IndirectBlock, 1, output, fileSize, ref written, progress);

        // Read double indirect blocks
        if (written < fileSize && inode.DoubleIndirectBlock != 0)
            WriteIndirectBlocks(inode.DoubleIndirectBlock, 2, output, fileSize, ref written, progress);

        // Read triple indirect blocks
        if (written < fileSize && inode.TripleIndirectBlock != 0)
            WriteIndirectBlocks(inode.TripleIndirectBlock, 3, output, fileSize, ref written, progress);
    }

    private void WriteIndirectBlocks(long blockAddr, int level, Stream output, long maxSize, ref long written, Action<long>? progress)
    {
        if (blockAddr == 0 || written >= maxSize) return;

        byte[] indirectBlock = ReadBlock(blockAddr);
        int pointersPerBlock = (int)(Superblock!.BlockSize / 8);

        for (int i = 0; i < pointersPerBlock && written < maxSize; i++)
        {
            long pointer = BinaryPrimitives.ReadInt64BigEndian(indirectBlock.AsSpan(i * 8));
            if (pointer == 0) continue;

            if (level == 1)
            {
                byte[] blockData = ReadBlock(pointer);
                int toWrite = (int)Math.Min(blockData.Length, maxSize - written);
                output.Write(blockData, 0, toWrite);
                written += toWrite;
                progress?.Invoke(written);
            }
            else
            {
                WriteIndirectBlocks(pointer, level - 1, output, maxSize, ref written, progress);
            }
        }
    }

    /// <summary>
    /// Read a data block at the given block address.
    /// </summary>
    private byte[] ReadBlock(long blockAddress)
    {
        long offset = _partitionOffsetBytes + (blockAddress * Superblock!.FragmentSize);

        if (offset < 0 || offset + Superblock.BlockSize > _disk.TotalSize)
        {
            throw new IOException(
                $"ReadBlock: address 0x{blockAddress:X} maps to offset 0x{offset:X} " +
                $"which is outside disk bounds (0 - 0x{_disk.TotalSize:X}).");
        }

        return _disk.ReadBytes(offset, (int)Superblock.BlockSize);
    }

    /// <summary>
    /// Recursively read indirect block chains.
    /// </summary>
    private void ReadIndirectBlocks(long blockAddr, int level, MemoryStream data, long maxSize)
    {
        if (blockAddr == 0 || data.Length >= maxSize) return;

        byte[] indirectBlock = ReadBlock(blockAddr);
        int pointersPerBlock = (int)(Superblock!.BlockSize / 8); // UFS2 uses 64-bit block pointers

        for (int i = 0; i < pointersPerBlock && data.Length < maxSize; i++)
        {
            long pointer = BinaryPrimitives.ReadInt64BigEndian(
                indirectBlock.AsSpan(i * 8));

            if (pointer == 0) continue;

            if (level == 1)
            {
                byte[] blockData = ReadBlock(pointer);
                int toWrite = (int)Math.Min(blockData.Length, maxSize - data.Length);
                data.Write(blockData, 0, toWrite);
            }
            else
            {
                ReadIndirectBlocks(pointer, level - 1, data, maxSize);
            }
        }
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
    public void ExtractFile(Ufs2Inode inode, string outputPath)
    {
        string? dir = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        using var fs = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
        ExtractInodeToStream(inode, fs);
    }

    /// <summary>
    /// Recursively extract a directory tree.
    /// </summary>
    public void ExtractDirectory(Ufs2Inode dirInode, string outputDir, IProgress<string>? progress = null)
    {
        Directory.CreateDirectory(outputDir);

        var entries = ReadDirectory(dirInode);
        foreach (var entry in entries)
        {
            if (entry.Name == "." || entry.Name == "..") continue;

            string outputPath = Path.Combine(outputDir, entry.Name);
            var inode = ReadInode(entry.InodeNumber);

            progress?.Report(outputPath);

            if (inode.FileType == Ufs2FileType.Directory)
            {
                ExtractDirectory(inode, outputPath, progress);
            }
            else if (inode.FileType == Ufs2FileType.RegularFile)
            {
                ExtractFile(inode, outputPath);
            }
        }
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

    public static Ufs2Superblock Parse(byte[] data)
    {
        var sb = new Ufs2Superblock();

        // The magic number is at offset 0x55C (1372) in the superblock
        if (data.Length > 0x560)
        {
            sb.Magic = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(0x55C));
        }

        if (!sb.IsValid) return sb;

        // Parse key fields (offsets from FreeBSD sys/ufs/ffs/fs.h)
        // PS3 uses big-endian byte order (PowerPC Cell)
        sb.BlockSize = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(0x30));       // fs_bsize
        sb.FragmentSize = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(0x34));    // fs_fsize
        sb.FragsPerGroup = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(0xBC));   // fs_fpg (NOT 0x74!)
        sb.InodesPerGroup = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(0xB8));  // fs_ipg
        sb.InodeBlockOffset = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(0x10));// fs_iblkno
        sb.TotalFragments = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x218)); // fs_size (UFS2 64-bit)
        sb.TotalDataFragments = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x220)); // fs_dsize
        sb.CylinderGroups = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(0x2C));  // fs_ncg (NOT 0xBC!)
        sb.InodeSize = 256; // UFS2 always uses 256-byte inodes

        // Volume name at offset 0x480 (fs_volname), 32 bytes max
        if (data.Length >= 0x4A0)
        {
            int nameEnd = Array.IndexOf(data, (byte)0, 0x480, 32);
            if (nameEnd < 0) nameEnd = 0x4A0;
            sb.VolumeName = Encoding.ASCII.GetString(data, 0x480, nameEnd - 0x480).TrimEnd('\0');
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

    public static Ufs2Inode Parse(byte[] data, long inodeNumber)
    {
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

        ushort mode = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(0x00));
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

        inode.LinkCount = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(0x02));
        inode.Uid = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(0x04));
        inode.Gid = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(0x08));
        inode.Size = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x10));
        inode.Blocks = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x18));

        // PS3 UFS2 inode timestamp layout (non-interleaved):
        //   0x20: di_atime (8B), 0x28: di_mtime (8B), 0x30: di_ctime (8B), 0x38: di_birthtime (8B)
        //   0x40-0x4F: nanosecond fields (4 x 4B)
        // Verified by comparing PS3-native inode hex dumps.
        inode.AccessTime = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x20));
        inode.ModifyTime = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x28));
        inode.ChangeTime = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x30));
        inode.CreateTime = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x38));

        inode.Flags = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(0x58));

        // Direct block pointers at offset 0x70 (12 x 8 bytes = 96 bytes)
        for (int i = 0; i < 12; i++)
            inode.DirectBlocks[i] = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0x70 + i * 8));

        // Indirect block pointers at offset 0xD0
        inode.IndirectBlock = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0xD0));
        inode.DoubleIndirectBlock = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0xD8));
        inode.TripleIndirectBlock = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0xE0));

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
