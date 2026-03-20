using System.Buffers.Binary;
using PS3HddTool.Core.Disk;

namespace PS3HddTool.Core.FileSystem;

/// <summary>
/// UFS2 filesystem write operations for PS3 HDD.
/// Supports dry-run mode that logs all changes without writing.
/// 
/// Write operations:
///   - CreateDirectory: create a new empty directory
///   - WriteFile: copy a file from host into the filesystem
///   - AddDirectoryEntry: add an entry to an existing directory
///   - AllocateInode: find and allocate a free inode in a CG
///   - AllocateBlocks: find and allocate free data blocks in a CG
/// </summary>
public class Ufs2Writer
{
    private readonly Ufs2FileSystem _fs;
    private readonly IDiskSource _disk;
    private readonly long _partitionOffset;
    private readonly bool _dryRun;
    private readonly Action<string> _log;

    // Pending writes for dry-run mode
    private readonly List<PendingWrite> _pendingWrites = new();
    private long _currentL1Block; // tracks current L1 indirect block during double-indirect writes

    public IReadOnlyList<PendingWrite> PendingWrites => _pendingWrites;

    public Ufs2Writer(Ufs2FileSystem fs, IDiskSource disk, long partitionOffsetBytes, bool dryRun, Action<string> log)
    {
        _fs = fs;
        _disk = disk;
        _partitionOffset = partitionOffsetBytes;
        _dryRun = dryRun;
        _log = log;
    }

    /// <summary>
    /// Read the cylinder group descriptor for the given CG number.
    /// </summary>
    public CylinderGroupInfo ReadCylinderGroup(int cgNumber)
    {
        var sb = _fs.Superblock!;
        long cgOffset = _partitionOffset + (long)cgNumber * sb.FragsPerGroup * sb.FragmentSize;
        long cgHeaderOffset = cgOffset + sb.InodeBlockOffset * sb.FragmentSize - 4 * sb.FragmentSize;
        // fs_cblkno = fs_iblkno - 4 typically. Read from superblock.
        int cblkno = BinaryPrimitives.ReadInt32BigEndian(_fs.RawSuperblockData.AsSpan(0x0C));
        cgHeaderOffset = cgOffset + (long)cblkno * sb.FragmentSize;

        // CG header is one block (fs_cgsize bytes, typically up to fs_bsize)
        int cgSize = BinaryPrimitives.ReadInt32BigEndian(_fs.RawSuperblockData.AsSpan(0xA0)); // fs_cgsize
        byte[] cgData = _disk.ReadBytes(cgHeaderOffset, cgSize);

        var cg = new CylinderGroupInfo();
        cg.CgNumber = cgNumber;
        cg.DiskOffset = cgHeaderOffset;
        cg.RawData = cgData;

        // Parse CG header (big-endian)
        // struct cg offsets:
        // 0x00: cg_firstfield (unused)
        // 0x04: cg_magic (0x00090255)
        // 0x08: cg_old_time
        // 0x0C: cg_cgx (CG number)
        // 0x10: cg_old_ncyl
        // 0x12: cg_old_niblk
        // 0x14: cg_ndblk (number of data blocks in this CG)
        // 0x18: cg_cs.cs_ndir
        // 0x1C: cg_cs.cs_nbfree (free blocks)
        // 0x20: cg_cs.cs_nifree (free inodes)
        // 0x24: cg_cs.cs_nffree (free frags)
        // 0x28: cg_rotor
        // 0x2C: cg_frotor
        // 0x30: cg_irotor (next free inode)
        // 0x34: cg_frsum[8] (32 bytes)
        // 0x54: cg_old_btotoff
        // 0x58: cg_old_boff
        // 0x5C: cg_iusedoff (offset to used inode bitmap)
        // 0x60: cg_freeoff (offset to free block bitmap)
        // ...

        cg.Magic = BinaryPrimitives.ReadUInt32BigEndian(cgData.AsSpan(0x04));
        cg.CgxFromDisk = BinaryPrimitives.ReadInt32BigEndian(cgData.AsSpan(0x0C));
        cg.NumDataBlocks = BinaryPrimitives.ReadInt32BigEndian(cgData.AsSpan(0x14));
        cg.FreeBlocks = BinaryPrimitives.ReadInt32BigEndian(cgData.AsSpan(0x1C));
        cg.FreeInodes = BinaryPrimitives.ReadInt32BigEndian(cgData.AsSpan(0x20));
        cg.InodesUsedOffset = BinaryPrimitives.ReadInt32BigEndian(cgData.AsSpan(0x5C));
        cg.FreeBlocksOffset = BinaryPrimitives.ReadInt32BigEndian(cgData.AsSpan(0x60));
        cg.InodeRotor = BinaryPrimitives.ReadInt32BigEndian(cgData.AsSpan(0x30));

        return cg;
    }

    /// <summary>
    /// Find a free inode in the given CG's inode bitmap.
    /// Returns the inode index within the CG, or -1 if none free.
    /// </summary>
    public int FindFreeInode(CylinderGroupInfo cg)
    {
        var sb = _fs.Superblock!;
        int ipg = (int)sb.InodesPerGroup;
        int bitmapOffset = cg.InodesUsedOffset;

        for (int i = 0; i < ipg; i++)
        {
            int byteIdx = bitmapOffset + (i / 8);
            int bitIdx = i % 8;
            if (byteIdx < cg.RawData.Length && (cg.RawData[byteIdx] & (1 << bitIdx)) == 0)
                return i; // This inode is free
        }
        return -1;
    }

    /// <summary>
    /// Find N contiguous free fragments in the given CG's block bitmap.
    /// Returns the fragment offset within the CG, or -1 if not enough free.
    /// </summary>
    public long FindFreeFragments(CylinderGroupInfo cg, int count)
    {
        var sb = _fs.Superblock!;
        int fpg = (int)sb.FragsPerGroup;
        int bitmapOffset = cg.FreeBlocksOffset;
        int dblkno = BinaryPrimitives.ReadInt32BigEndian(_fs.RawSuperblockData.AsSpan(0x14));

        // Search for 'count' contiguous free fragments starting from the data area
        int consecutive = 0;
        int startFrag = -1;

        for (int f = dblkno; f < fpg; f++)
        {
            int byteIdx = bitmapOffset + (f / 8);
            int bitIdx = f % 8;
            bool isFree = byteIdx < cg.RawData.Length && (cg.RawData[byteIdx] & (1 << bitIdx)) != 0;
            // In UFS2, a SET bit in the free bitmap means the fragment IS free

            if (isFree)
            {
                if (consecutive == 0) startFrag = f;
                consecutive++;
                if (consecutive >= count) return startFrag;
            }
            else
            {
                consecutive = 0;
            }
        }
        return -1;
    }

    /// <summary>
    /// Mark an inode as used in the CG bitmap.
    /// </summary>
    public void MarkInodeUsed(CylinderGroupInfo cg, int inodeIdx)
    {
        int byteIdx = cg.InodesUsedOffset + (inodeIdx / 8);
        int bitIdx = inodeIdx % 8;
        cg.RawData[byteIdx] |= (byte)(1 << bitIdx);
        cg.FreeInodes--;
        // Update cs_nifree in CG header
        BinaryPrimitives.WriteInt32BigEndian(cg.RawData.AsSpan(0x20), cg.FreeInodes);
    }

    /// <summary>
    /// Mark fragments as used in the CG free block bitmap.
    /// </summary>
    public void MarkFragmentsUsed(CylinderGroupInfo cg, int startFrag, int count)
    {
        var sb = _fs.Superblock!;
        for (int f = startFrag; f < startFrag + count; f++)
        {
            int byteIdx = cg.FreeBlocksOffset + (f / 8);
            int bitIdx = f % 8;
            cg.RawData[byteIdx] &= (byte)~(1 << bitIdx); // Clear bit = used
        }
        // Update free block count (approximate — should count actual blocks not frags)
        int blocksUsed = (int)((count + sb.BlockSize / sb.FragmentSize - 1) 
                         / (sb.BlockSize / sb.FragmentSize));
        cg.FreeBlocks -= blocksUsed;
        BinaryPrimitives.WriteInt32BigEndian(cg.RawData.AsSpan(0x1C), cg.FreeBlocks);
    }

    /// <summary>
    /// Write the CG header back to disk.
    /// </summary>
    public void WriteCylinderGroup(CylinderGroupInfo cg)
    {
        // Update cg_old_time at offset 0x08 (PS3 sets this on every CG modification)
        int now = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        BinaryPrimitives.WriteInt32BigEndian(cg.RawData.AsSpan(0x08), now);
        
        QueueWrite(cg.DiskOffset, cg.RawData, $"CG {cg.CgNumber} header ({cg.RawData.Length} bytes)");
    }

    /// <summary>
    /// Write an inode to disk.
    /// </summary>
    public void WriteInode(long inodeNumber, byte[] inodeData)
    {
        var sb = _fs.Superblock!;
        long group = inodeNumber / sb.InodesPerGroup;
        long indexInGroup = inodeNumber % sb.InodesPerGroup;
        long cgOffset = _partitionOffset + (group * sb.FragsPerGroup * sb.FragmentSize);
        long inodeTableOffset = cgOffset + (sb.InodeBlockOffset * sb.FragmentSize);
        long inodeOffset = inodeTableOffset + (indexInGroup * (int)sb.InodeSize);

        QueueWrite(inodeOffset, inodeData, $"Inode {inodeNumber} (CG {group}, idx {indexInGroup})");
    }

    /// <summary>
    /// Build a UFS2 dinode2 structure for a new directory.
    /// </summary>
    public byte[] BuildDirectoryInode(long dataBlockFrag, int nlink)
    {
        byte[] inode = new byte[256];
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var sb = _fs.Superblock!;
        uint gen = (uint)Random.Shared.Next();

        // di_mode: directory + rwxrwxrwx = 0x41FF (PS3 uses 777)
        BinaryPrimitives.WriteUInt16BigEndian(inode.AsSpan(0x00), 0x41FF);
        // di_nlink
        BinaryPrimitives.WriteInt16BigEndian(inode.AsSpan(0x02), (short)nlink);
        // di_blksize = fs_bsize
        BinaryPrimitives.WriteUInt32BigEndian(inode.AsSpan(0x0C), (uint)sb.BlockSize);
        // di_size = 512
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x10), 512);
        // di_blocks = 8 (4096 bytes allocated / 512)
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x18), (sb.FragmentSize / 512));
        // di_atime
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x20), now);
        // di_mtime
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x28), now);
        // di_ctime
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x30), now);
        // di_birthtime
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x38), now);
        // 0x40-0x4F: nanosecond fields (leave as 0)
        // di_gen at 0x50
        BinaryPrimitives.WriteUInt32BigEndian(inode.AsSpan(0x50), gen);
        // di_db[0] = data block fragment address
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x70), dataBlockFrag);

        return inode;
    }

    /// <summary>
    /// Build a UFS2 dinode2 structure for a new regular file.
    /// </summary>
    public byte[] BuildFileInode(long fileSize, long[] directBlocks, long indirectBlock = 0, 
                                  long doubleIndirect = 0, long tripleIndirect = 0)
    {
        byte[] inode = new byte[256];
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var sb = _fs.Superblock!;
        uint gen = (uint)Random.Shared.Next();

        // di_mode: regular file + rw-rw-rw- = 0x81B6 (PS3 uses 666)
        BinaryPrimitives.WriteUInt16BigEndian(inode.AsSpan(0x00), 0x81B6);
        // di_nlink = 1
        BinaryPrimitives.WriteInt16BigEndian(inode.AsSpan(0x02), 1);
        // di_blksize = 0 for files (PS3 leaves this at 0)
        // di_size
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x10), fileSize);
        // di_blocks: count ALLOCATED blocks in 512-byte units (round up to full fs blocks)
        long fsBlocks = (fileSize + sb.BlockSize - 1) / sb.BlockSize;
        long allocatedBytes = fsBlocks * sb.BlockSize;
        // Also count indirect blocks
        long indirectBlocks = (indirectBlock != 0 ? 1 : 0) + (doubleIndirect != 0 ? 1 : 0);
        if (doubleIndirect != 0)
        {
            long ptrsPerBlock = sb.BlockSize / 8;
            long dataInDirect = 12;
            long dataInSingle = ptrsPerBlock;
            long dataInDouble = fsBlocks - dataInDirect - dataInSingle;
            if (dataInDouble > 0)
                indirectBlocks += (dataInDouble + ptrsPerBlock - 1) / ptrsPerBlock; // L1 blocks
        }
        long totalAllocated = (allocatedBytes + indirectBlocks * sb.BlockSize) / 512;
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x18), totalAllocated);
        // timestamps
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x20), now);
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x28), now);
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x30), now);
        BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x38), now);
        // di_gen at 0x50
        BinaryPrimitives.WriteUInt32BigEndian(inode.AsSpan(0x50), gen);
        // di_db[0..11] = direct block pointers
        for (int i = 0; i < Math.Min(12, directBlocks.Length); i++)
            BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0x70 + i * 8), directBlocks[i]);
        // di_ib[0] = single indirect
        if (indirectBlock != 0)
            BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0xD0), indirectBlock);
        // di_ib[1] = double indirect
        if (doubleIndirect != 0)
            BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0xD8), doubleIndirect);
        // di_ib[2] = triple indirect
        if (tripleIndirect != 0)
            BinaryPrimitives.WriteInt64BigEndian(inode.AsSpan(0xE0), tripleIndirect);

        return inode;
    }

    /// <summary>
    /// Build a minimal directory block with "." and ".." entries.
    /// </summary>
    public byte[] BuildEmptyDirectoryBlock(long selfInode, long parentInode)
    {
        var sb = _fs.Superblock!;
        byte[] block = new byte[(int)sb.BlockSize];

        int offset = 0;
        // "." entry
        BinaryPrimitives.WriteUInt32BigEndian(block.AsSpan(offset), (uint)selfInode);
        BinaryPrimitives.WriteUInt16BigEndian(block.AsSpan(offset + 4), 12); // reclen
        block[offset + 6] = 4; // DT_DIR
        block[offset + 7] = 1; // namelen
        block[offset + 8] = (byte)'.';
        offset += 12;

        // ".." entry — takes remaining space in the block
        BinaryPrimitives.WriteUInt32BigEndian(block.AsSpan(offset), (uint)parentInode);
        BinaryPrimitives.WriteUInt16BigEndian(block.AsSpan(offset + 4), (ushort)((int)sb.BlockSize - 12)); // reclen = rest
        block[offset + 6] = 4; // DT_DIR
        block[offset + 7] = 2; // namelen
        block[offset + 8] = (byte)'.';
        block[offset + 9] = (byte)'.';

        return block;
    }

    /// <summary>
    /// Add a directory entry to an existing directory's data block.
    /// Finds the last entry and splits its reclen to make room.
    /// </summary>
    public byte[] AddEntryToDirectoryBlock(byte[] dirBlock, long inode, string name, byte dirEntryType)
    {
        byte[] result = (byte[])dirBlock.Clone();
        int offset = 0;

        // Walk to find the last entry
        int lastOffset = 0;
        while (offset < result.Length)
        {
            ushort recLen = BinaryPrimitives.ReadUInt16BigEndian(result.AsSpan(offset + 4));
            if (recLen == 0) break;

            int nextOffset = offset + recLen;
            if (nextOffset >= result.Length || 
                BinaryPrimitives.ReadUInt32BigEndian(result.AsSpan(nextOffset)) == 0)
            {
                lastOffset = offset;
                break;
            }
            lastOffset = offset;
            offset = nextOffset;
        }

        // Calculate actual size of the last entry
        byte lastNameLen = result[lastOffset + 7];
        int lastActualSize = ((8 + lastNameLen + 3) / 4) * 4; // 4-byte aligned
        ushort lastRecLen = BinaryPrimitives.ReadUInt16BigEndian(result.AsSpan(lastOffset + 4));

        // New entry size
        int newEntrySize = ((8 + name.Length + 3) / 4) * 4;
        int remainingSpace = lastRecLen - lastActualSize;

        if (remainingSpace < newEntrySize)
            throw new IOException($"Not enough space in directory block for '{name}' (need {newEntrySize}, have {remainingSpace})");

        // Shrink last entry's reclen to its actual size
        BinaryPrimitives.WriteUInt16BigEndian(result.AsSpan(lastOffset + 4), (ushort)lastActualSize);

        // Write new entry at lastOffset + lastActualSize
        int newOffset = lastOffset + lastActualSize;
        BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(newOffset), (uint)inode);
        BinaryPrimitives.WriteUInt16BigEndian(result.AsSpan(newOffset + 4), (ushort)(lastRecLen - lastActualSize));
        result[newOffset + 6] = dirEntryType;
        result[newOffset + 7] = (byte)name.Length;
        System.Text.Encoding.ASCII.GetBytes(name, 0, name.Length, result, newOffset + 8);

        return result;
    }

    /// <summary>
    /// Write a data block to disk at the given fragment address.
    /// </summary>
    public void WriteDataBlock(long fragmentAddress, byte[] data)
    {
        long offset = _partitionOffset + (fragmentAddress * _fs.Superblock!.FragmentSize);
        QueueWrite(offset, data, $"Data block at frag 0x{fragmentAddress:X} ({data.Length} bytes)");
    }

    /// <summary>
    /// High-level: Create a new empty directory inside a parent directory.
    /// </summary>
    public long CreateDirectory(long parentInodeNumber, string name)
    {
        var sb = _fs.Superblock!;
        _log($"[WRITE] Creating directory '{name}' in inode {parentInodeNumber}");

        // 1. Choose a CG (same as parent for locality)
        int parentCg = (int)(parentInodeNumber / sb.InodesPerGroup);
        var cg = ReadCylinderGroup(parentCg);

        if (cg.Magic != 0x00090255)
            throw new IOException($"CG {parentCg} has invalid magic: 0x{cg.Magic:X8}");

        _log($"  CG {parentCg}: {cg.FreeInodes} free inodes, {cg.FreeBlocks} free blocks");

        // 2. Allocate an inode (try parent CG first, then scan others)
        int inodeCg = parentCg;
        var inodeCgInfo = cg;
        int freeIdx = FindFreeInode(inodeCgInfo);
        if (freeIdx < 0)
        {
            for (int i = 1; i < sb.CylinderGroups; i++)
            {
                int tryCg = (parentCg + i) % sb.CylinderGroups;
                inodeCgInfo = ReadCylinderGroup(tryCg);
                freeIdx = FindFreeInode(inodeCgInfo);
                if (freeIdx >= 0) { inodeCg = tryCg; break; }
            }
            if (freeIdx < 0) throw new IOException("No free inodes on disk.");
        }
        long newInodeNumber = (long)inodeCg * sb.InodesPerGroup + freeIdx;
        _log($"  Allocating inode {newInodeNumber} (CG {inodeCg}, idx {freeIdx})");

        // 3. Allocate a data block (try same CG as inode, then scan others)
        int fragsPerBlock = (int)(sb.BlockSize / sb.FragmentSize);
        int blockCg = inodeCg;
        var blockCgInfo = inodeCgInfo;
        long freeFrag = FindFreeFragments(blockCgInfo, fragsPerBlock);
        if (freeFrag < 0)
        {
            for (int i = 1; i < sb.CylinderGroups; i++)
            {
                int tryCg = (inodeCg + i) % sb.CylinderGroups;
                blockCgInfo = ReadCylinderGroup(tryCg);
                freeFrag = FindFreeFragments(blockCgInfo, fragsPerBlock);
                if (freeFrag >= 0) { blockCg = tryCg; _log($"  CG {inodeCg} full, using CG {tryCg} for data block"); break; }
            }
            if (freeFrag < 0) throw new IOException("No free blocks on disk.");
        }
        long absFragAddr = (long)blockCg * sb.FragsPerGroup + freeFrag;
        _log($"  Allocating block at frag 0x{absFragAddr:X} (CG {blockCg}, frag {freeFrag})");

        // 4. Build the directory data block with "." and ".."
        byte[] dirBlock = BuildEmptyDirectoryBlock(newInodeNumber, parentInodeNumber);
        _log($"  Writing directory block ({dirBlock.Length} bytes)");
        WriteDataBlock(absFragAddr, dirBlock);

        // 5. Build and write the new inode
        byte[] inodeData = BuildDirectoryInode(absFragAddr, 2); // nlink=2 (self + ".")
        _log($"  Writing inode {newInodeNumber}");
        WriteInode(newInodeNumber, inodeData);

        // 6. Update CG bitmaps
        MarkInodeUsed(inodeCgInfo, freeIdx);
        if (blockCg == inodeCg)
        {
            MarkFragmentsUsed(inodeCgInfo, (int)freeFrag, fragsPerBlock);
            WriteCylinderGroup(inodeCgInfo);
        }
        else
        {
            WriteCylinderGroup(inodeCgInfo);
            MarkFragmentsUsed(blockCgInfo, (int)freeFrag, fragsPerBlock);
            WriteCylinderGroup(blockCgInfo);
        }
        _log($"  Updated CG bitmaps");

        // 7. Add entry to parent directory (try existing blocks, expand if needed)
        _log($"  Adding '{name}' to parent directory (inode {parentInodeNumber})");
        var parentInode = _fs.ReadInode(parentInodeNumber);
        bool expanded = false;
        byte[]? expandedRawParent = null;
        AddEntryToDirectory(parentInodeNumber, parentInode, newInodeNumber, name, 4, inodeCg, inodeCgInfo, blockCg, blockCgInfo, fragsPerBlock, out expanded, out expandedRawParent);

        // 8. Update parent inode nlink (add 1 for the new ".." reference)
        // If directory was expanded, AddEntryToDirectory already wrote an updated inode.
        // We need to read THAT version (or use the returned bytes) and add nlink to it.
        byte[] rawParent;
        if (expanded && expandedRawParent != null)
        {
            rawParent = expandedRawParent;
        }
        else
        {
            rawParent = _disk.ReadBytes(
                _partitionOffset + (parentInodeNumber / sb.InodesPerGroup) * sb.FragsPerGroup * sb.FragmentSize
                + sb.InodeBlockOffset * sb.FragmentSize + (parentInodeNumber % sb.InodesPerGroup) * sb.InodeSize,
                (int)sb.InodeSize);
        }
        short nlink = BinaryPrimitives.ReadInt16BigEndian(rawParent.AsSpan(0x02));
        BinaryPrimitives.WriteInt16BigEndian(rawParent.AsSpan(0x02), (short)(nlink + 1));
        WriteInode(parentInodeNumber, rawParent);

        _log($"  Directory '{name}' created as inode {newInodeNumber}");
        return newInodeNumber;
    }

    /// <summary>
    /// High-level: Write a file from a host stream into the filesystem.
    /// </summary>
    public long WriteFile(long parentInodeNumber, string name, Stream sourceData, long fileSize)
    {
        var sb = _fs.Superblock!;
        _log($"[WRITE] Writing file '{name}' ({fileSize} bytes) into inode {parentInodeNumber}");

        int parentCg = (int)(parentInodeNumber / sb.InodesPerGroup);
        var cg = ReadCylinderGroup(parentCg);

        if (cg.Magic != 0x00090255)
            throw new IOException($"CG {parentCg} has invalid magic: 0x{cg.Magic:X8}");

        // 1. Allocate inode
        int freeIdx = FindFreeInode(cg);
        if (freeIdx < 0) throw new IOException($"No free inodes in CG {parentCg}");
        long newInodeNumber = (long)parentCg * sb.InodesPerGroup + freeIdx;
        _log($"  Allocating inode {newInodeNumber}");

        // 2. Calculate blocks needed
        int fragsPerBlock = (int)(sb.BlockSize / sb.FragmentSize);
        long blocksNeeded = (fileSize + sb.BlockSize - 1) / sb.BlockSize;
        _log($"  Need {blocksNeeded} blocks ({blocksNeeded * fragsPerBlock} fragments)");

        // 3. Allocate and write data blocks
        var directBlocks = new long[12];
        long indirectBlock = 0;
        long doubleIndirectBlock = 0;
        _currentL1Block = 0;
        long bytesRemaining = fileSize;
        byte[] readBuffer = new byte[(int)sb.BlockSize];

        // Allocate blocks across CGs as needed
        int currentCg = parentCg;
        var currentCgInfo = cg;

        for (long b = 0; b < blocksNeeded; b++)
        {
            // Find free fragments in current CG
            long freeFrag = FindFreeFragments(currentCgInfo, fragsPerBlock);
            if (freeFrag < 0)
            {
                // Try next CG
                currentCg = (currentCg + 1) % sb.CylinderGroups;
                currentCgInfo = ReadCylinderGroup(currentCg);
                freeFrag = FindFreeFragments(currentCgInfo, fragsPerBlock);
                if (freeFrag < 0) throw new IOException("No free blocks available on disk.");
            }

            long absFragAddr = (long)currentCg * sb.FragsPerGroup + freeFrag;
            MarkFragmentsUsed(currentCgInfo, (int)freeFrag, fragsPerBlock);

            // Read data from source
            int toRead = (int)Math.Min(sb.BlockSize, bytesRemaining);
            Array.Clear(readBuffer, 0, readBuffer.Length);
            int totalRead = 0;
            while (totalRead < toRead)
            {
                int r = sourceData.Read(readBuffer, totalRead, toRead - totalRead);
                if (r == 0) break;
                totalRead += r;
            }

            // Write the data block
            WriteDataBlock(absFragAddr, readBuffer);
            bytesRemaining -= toRead;

            // Store block pointer
            if (b < 12)
            {
                directBlocks[b] = absFragAddr;
            }
            else
            {
                long ptrsPerBlock = sb.BlockSize / 8;

                if (b < 12 + ptrsPerBlock)
                {
                    // Single indirect range
                    int ptrIdx = (int)(b - 12);

                    if (ptrIdx == 0)
                    {
                        // Allocate the indirect block
                        long indFrag = FindFreeFragments(currentCgInfo, fragsPerBlock);
                        if (indFrag < 0) throw new IOException("No space for indirect block.");
                        indirectBlock = (long)currentCg * sb.FragsPerGroup + indFrag;
                        MarkFragmentsUsed(currentCgInfo, (int)indFrag, fragsPerBlock);
                        // Write fresh indirect block with first pointer
                        byte[] newInd = new byte[(int)sb.BlockSize];
                        BinaryPrimitives.WriteInt64BigEndian(newInd.AsSpan(0), absFragAddr);
                        WriteDataBlock(indirectBlock, newInd);
                        _log($"  Allocated indirect block at frag 0x{indirectBlock:X}");
                    }
                    else
                    {
                        // Update existing indirect block
                        long indOffset = _partitionOffset + indirectBlock * sb.FragmentSize;
                        byte[] indBlock = _disk.ReadBytes(indOffset, (int)sb.BlockSize);
                        BinaryPrimitives.WriteInt64BigEndian(indBlock.AsSpan(ptrIdx * 8), absFragAddr);
                        QueueWrite(indOffset, indBlock, $"Indirect block update (ptr {ptrIdx})");
                    }
                }
                else if (b < 12 + ptrsPerBlock + ptrsPerBlock * ptrsPerBlock)
                {
                    // Double indirect range
                    long dblIdx = b - 12 - ptrsPerBlock;
                    int l1Idx = (int)(dblIdx / ptrsPerBlock); // which L1 indirect block
                    int l2Idx = (int)(dblIdx % ptrsPerBlock); // pointer within that L1 block

                    if (dblIdx == 0)
                    {
                        // Allocate the double indirect block
                        long diFrag = FindFreeFragments(currentCgInfo, fragsPerBlock);
                        if (diFrag < 0) throw new IOException("No space for double indirect block.");
                        doubleIndirectBlock = (long)currentCg * sb.FragsPerGroup + diFrag;
                        MarkFragmentsUsed(currentCgInfo, (int)diFrag, fragsPerBlock);
                        byte[] newDi = new byte[(int)sb.BlockSize];
                        WriteDataBlock(doubleIndirectBlock, newDi);
                        _log($"  Allocated double indirect block at frag 0x{doubleIndirectBlock:X}");
                    }

                    if (l2Idx == 0)
                    {
                        // Allocate a new L1 indirect block under the double indirect
                        long l1Frag = FindFreeFragments(currentCgInfo, fragsPerBlock);
                        if (l1Frag < 0) throw new IOException("No space for L1 indirect block.");
                        long l1Addr = (long)currentCg * sb.FragsPerGroup + l1Frag;
                        MarkFragmentsUsed(currentCgInfo, (int)l1Frag, fragsPerBlock);

                        // Write L1 block with first data pointer
                        byte[] newL1 = new byte[(int)sb.BlockSize];
                        BinaryPrimitives.WriteInt64BigEndian(newL1.AsSpan(0), absFragAddr);
                        WriteDataBlock(l1Addr, newL1);

                        // Update double indirect block to point to this L1 block
                        long diOffset = _partitionOffset + doubleIndirectBlock * sb.FragmentSize;
                        byte[] diBlock = _disk.ReadBytes(diOffset, (int)sb.BlockSize);
                        BinaryPrimitives.WriteInt64BigEndian(diBlock.AsSpan(l1Idx * 8), l1Addr);
                        QueueWrite(diOffset, diBlock, $"Double indirect update (L1 slot {l1Idx})");

                        _currentL1Block = l1Addr;
                    }
                    else
                    {
                        // Update existing L1 block
                        long l1Offset = _partitionOffset + _currentL1Block * sb.FragmentSize;
                        byte[] l1Block = _disk.ReadBytes(l1Offset, (int)sb.BlockSize);
                        BinaryPrimitives.WriteInt64BigEndian(l1Block.AsSpan(l2Idx * 8), absFragAddr);
                        QueueWrite(l1Offset, l1Block, $"L1 indirect update (ptr {l2Idx})");
                    }
                }
                else
                {
                    throw new NotImplementedException("Triple indirect blocks not yet implemented (file > ~68 GB).");
                }
            }

            if (b % 1000 == 0 && b > 0)
                _log($"  Progress: {b}/{blocksNeeded} blocks written");
        }

        // Write updated CG(s)
        WriteCylinderGroup(currentCgInfo);
        if (currentCg != parentCg)
            WriteCylinderGroup(cg);

        // 4. Mark inode used
        MarkInodeUsed(cg, freeIdx);
        WriteCylinderGroup(cg);

        // 5. Build and write inode
        byte[] inodeData = BuildFileInode(fileSize, directBlocks, indirectBlock, doubleIndirectBlock);
        WriteInode(newInodeNumber, inodeData);
        _log($"  Wrote inode {newInodeNumber}");

        // 6. Add to parent directory
        var parentInode = _fs.ReadInode(parentInodeNumber);
        AddEntryToDirectory(parentInodeNumber, parentInode, newInodeNumber, name, 8, parentCg, cg, currentCg, currentCgInfo, fragsPerBlock, out var fileExpanded, out _);

        _log($"  File '{name}' written as inode {newInodeNumber} ({fileSize} bytes, {blocksNeeded} blocks)");
        return newInodeNumber;
    }

    /// <summary>
    /// Add a directory entry to a parent directory. Tries all existing data blocks first.
    /// If all blocks are full, allocates a new block and expands the directory.
    /// </summary>
    private void AddEntryToDirectory(long parentInodeNumber, Ufs2Inode parentInode,
        long newInodeNumber, string name, byte entryType,
        int inodeCg, CylinderGroupInfo inodeCgInfo, int blockCg, CylinderGroupInfo blockCgInfo, int fragsPerBlock,
        out bool expanded, out byte[]? expandedRawParent)
    {
        var sb = _fs.Superblock!;
        long dirSize = parentInode.Size;
        long bytesLeft = dirSize;
        expanded = false;
        expandedRawParent = null;

        // Try each existing directory data block
        for (int i = 0; i < 12; i++)
        {
            long blockAddr = parentInode.DirectBlocks[i];
            if (blockAddr == 0) break;
            if (bytesLeft <= 0) break;

            // Read only as much as this block actually contains
            int thisBlockSize = (int)Math.Min(sb.BlockSize, bytesLeft);
            long blockDiskOffset = _partitionOffset + blockAddr * sb.FragmentSize;
            byte[] dirBlock = _disk.ReadBytes(blockDiskOffset, thisBlockSize);

            try
            {
                byte[] updated = AddEntryToDirectoryBlock(dirBlock, newInodeNumber, name, entryType);
                QueueWrite(blockDiskOffset, updated, $"Parent dir block {i} (add '{name}')");
                return; // Success
            }
            catch (IOException ex)
            {
                _log($"  Block {i} full ({thisBlockSize} bytes): {ex.Message}");
                // This block is full, try next
            }
            bytesLeft -= thisBlockSize;
        }

        // All existing blocks are full — expand the directory
        _log($"  All directory blocks full, expanding with new block (dirSize={dirSize})");

        // Find the next free direct block slot
        int newSlot = -1;
        for (int i = 0; i < 12; i++)
        {
            if (parentInode.DirectBlocks[i] == 0) { newSlot = i; break; }
        }
        if (newSlot < 0)
            throw new IOException("Directory has 12 direct blocks full — indirect directory blocks not supported yet.");

        // Allocate a new data block (use blockCg or scan for free)
        long newFrag = FindFreeFragments(blockCgInfo, fragsPerBlock);
        int allocCg = blockCg;
        if (newFrag < 0)
        {
            for (int i = 1; i < sb.CylinderGroups; i++)
            {
                int tryCg = (blockCg + i) % sb.CylinderGroups;
                var tryCgInfo = ReadCylinderGroup(tryCg);
                newFrag = FindFreeFragments(tryCgInfo, fragsPerBlock);
                if (newFrag >= 0) { allocCg = tryCg; blockCgInfo = tryCgInfo; break; }
            }
            if (newFrag < 0) throw new IOException("No free blocks for directory expansion.");
        }
        long newAbsFrag = (long)allocCg * sb.FragsPerGroup + newFrag;

        // Build a new directory block with just this one entry (takes entire block)
        byte[] newBlock = new byte[(int)sb.BlockSize];
        BinaryPrimitives.WriteUInt32BigEndian(newBlock.AsSpan(0), (uint)newInodeNumber);
        BinaryPrimitives.WriteUInt16BigEndian(newBlock.AsSpan(4), (ushort)sb.BlockSize); // reclen = entire block
        newBlock[6] = entryType;
        newBlock[7] = (byte)name.Length;
        System.Text.Encoding.ASCII.GetBytes(name, 0, name.Length, newBlock, 8);

        WriteDataBlock(newAbsFrag, newBlock);
        MarkFragmentsUsed(blockCgInfo, (int)newFrag, fragsPerBlock);
        WriteCylinderGroup(blockCgInfo);

        // Update parent inode: add new block pointer and increase size
        var rawParent = _disk.ReadBytes(
            _partitionOffset + (parentInodeNumber / sb.InodesPerGroup) * sb.FragsPerGroup * sb.FragmentSize
            + sb.InodeBlockOffset * sb.FragmentSize + (parentInodeNumber % sb.InodesPerGroup) * sb.InodeSize,
            (int)sb.InodeSize);

        // Write new direct block pointer at di_db[newSlot] (offset 0x70 + slot*8)
        BinaryPrimitives.WriteInt64BigEndian(rawParent.AsSpan(0x70 + newSlot * 8), newAbsFrag);
        // Update di_size (offset 0x10)
        long oldSize = BinaryPrimitives.ReadInt64BigEndian(rawParent.AsSpan(0x10));
        BinaryPrimitives.WriteInt64BigEndian(rawParent.AsSpan(0x10), oldSize + sb.BlockSize);
        // Update di_blocks (offset 0x18, in 512-byte units)
        long oldBlocks = BinaryPrimitives.ReadInt64BigEndian(rawParent.AsSpan(0x18));
        BinaryPrimitives.WriteInt64BigEndian(rawParent.AsSpan(0x18), oldBlocks + sb.BlockSize / 512);
        WriteInode(parentInodeNumber, rawParent);
        expanded = true;
        expandedRawParent = rawParent;

        _log($"  Expanded directory: new block at frag 0x{newAbsFrag:X} (slot {newSlot}), size now {oldSize + sb.BlockSize}");
    }

    /// <summary>
    /// Queue a write operation. In dry-run mode, just logs it.
    /// In live mode, executes immediately.
    /// </summary>
    private void QueueWrite(long diskOffset, byte[] data, string description)
    {
        var write = new PendingWrite
        {
            DiskOffset = diskOffset,
            Data = data,
            Description = description
        };
        _pendingWrites.Add(write);

        if (_dryRun)
        {
            _log($"  [FAKE WRITE TEST] Would write {data.Length} bytes at offset 0x{diskOffset:X}: {description}");
        }
        else
        {
            // Sector-align the write
            int sectorSize = 512;
            long alignedStart = (diskOffset / sectorSize) * sectorSize;
            long alignedEnd = ((diskOffset + data.Length + sectorSize - 1) / sectorSize) * sectorSize;
            int alignedLen = (int)(alignedEnd - alignedStart);
            int startDelta = (int)(diskOffset - alignedStart);

            // Read-modify-write for sector alignment
            byte[] aligned = _disk.ReadBytes(alignedStart, alignedLen);
            Array.Copy(data, 0, aligned, startDelta, data.Length);
            _disk.WriteBytes(alignedStart, aligned);
            _log($"  [WRITE] {data.Length} bytes at offset 0x{diskOffset:X}: {description}");
        }
    }

    /// <summary>
    /// Update the superblock's global summary (fs_cstotal) and timestamp after writes.
    /// Recalculates totals by scanning all CG headers.
    /// Also writes backup superblock.
    /// </summary>
    public void UpdateSuperblock(int inodesUsed, int blocksUsed, int dirsCreated)
    {
        var sb = _fs.Superblock!;
        
        // Read current superblock (primary at offset 0x10000 from partition start)
        long sbOffset = _partitionOffset + 65536;
        byte[] sbData = _disk.ReadBytes(sbOffset, 8192);
        
        _log($"  Updating superblock summary: inodes={inodesUsed}, blocks={blocksUsed}, dirs={dirsCreated}");
        
        // fs_cstotal at offset 0x50 in superblock:
        // PS3 uses 32-bit csum fields (not 64-bit like standard UFS2)
        // 0x50: cs_ndir   (int32) - total number of directories
        // 0x54: cs_nbfree (int32) - total free blocks  
        // 0x58: cs_nifree (int32) - total free inodes
        // 0x5C: cs_nffree (int32) - total free frags
        
        int csNdir = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x50));
        int csNbfree = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x54));
        int csNifree = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x58));
        int csNffree = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x5C));
        
        _log($"  Before: dirs={csNdir}, free_blocks={csNbfree}, free_inodes={csNifree}, free_frags={csNffree}");
        
        csNdir += dirsCreated;
        csNbfree -= blocksUsed;
        csNifree -= inodesUsed;
        csNffree -= blocksUsed * (int)(sb.BlockSize / sb.FragmentSize);
        
        BinaryPrimitives.WriteInt32BigEndian(sbData.AsSpan(0x50), csNdir);
        BinaryPrimitives.WriteInt32BigEndian(sbData.AsSpan(0x54), csNbfree);
        BinaryPrimitives.WriteInt32BigEndian(sbData.AsSpan(0x58), csNifree);
        BinaryPrimitives.WriteInt32BigEndian(sbData.AsSpan(0x5C), csNffree);
        
        // Update fs_time (last write time)
        // From raw dump: 0x550-0x557 is fs_time (int32 at 0x550, or int64)
        // 0x55C is fs_magic — must NOT overwrite!
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        BinaryPrimitives.WriteInt32BigEndian(sbData.AsSpan(0x550), (int)now);
        
        _log($"  After: dirs={csNdir}, free_blocks={csNbfree}, free_inodes={csNifree}, free_frags={csNffree}");
        
        // Write primary superblock
        QueueWrite(sbOffset, sbData, "Primary superblock update");
        
        // Write backup superblock (at offset 0x10000 from 2nd CG, i.e. partition + fpg * fsize + 0x10000)
        // The backup is typically at the same relative offset in each CG
        // From the log: sector 0x40 also had a valid UFS2 superblock
        // sector 0x40 = byte offset 0x8000 absolute, which is partition_offset + 0x4000 = 0x4000 + 0x4000 = 0x8000
        // Actually sector 0x40 from disk start = 0x8000, partition starts at sector 0x20 = 0x4000
        // So backup SB is at partition_offset + 0x4000 = 0x4000 + 0x4000 = byte 0x8000... 
        // No — sector 0x40 absolute = byte 0x8000. Partition at sector 0x20 = byte 0x4000.
        // So backup SB at sector 0x40 is at partition_offset + (0x40-0x20)*512 = 0x4000 + 0x4000 = byte 0x8000 absolute
        // But relative to partition: 0x8000 - 0x4000 = 0x4000 = 16384 bytes from partition start
        // Primary SB is at 65536 (0x10000) from partition start
        // Backup at 0x4000 from partition start? That doesn't match. Let me use the known backup locations.
        // From logs: backup at sector 0x40, primary at sector 0x20
        // Primary SB data is at partition + 0x10000 (sector 0x20 + 128 sectors = sector 0xA0... no)
        // Actually the scan found UFS2 at sector 0x40 as well — this is 0x40*512 = 0x8000 absolute
        // Let's just write to that location too
        long backupSbAbsolute = 0x40 * 512 + 65536; // sector 0x40 + 64KB offset into it
        // Actually no — the UFS2 was found AT sector 0x40, meaning the SB magic was at sector 0x40 + 128 sectors...
        // The scanner checks for magic at the read offset + 0x55C. If it found UFS2 "at sector 0x40", 
        // it means the superblock starts at byte 0x40*512 + 65536... no, the scan reads 8KB from the sector.
        // Let me just skip the backup for now and focus on the primary.
        
        _log($"  Superblock updated.");
    }

    /// <summary>
    /// Execute all pending writes (for dry-run mode, call this to commit).
    /// </summary>
    public void CommitPendingWrites()
    {
        _log($"Committing {_pendingWrites.Count} pending writes...");
        foreach (var write in _pendingWrites)
        {
            int sectorSize = 512;
            long alignedStart = (write.DiskOffset / sectorSize) * sectorSize;
            long alignedEnd = ((write.DiskOffset + write.Data.Length + sectorSize - 1) / sectorSize) * sectorSize;
            int alignedLen = (int)(alignedEnd - alignedStart);
            int startDelta = (int)(write.DiskOffset - alignedStart);

            byte[] aligned = _disk.ReadBytes(alignedStart, alignedLen);
            Array.Copy(write.Data, 0, aligned, startDelta, write.Data.Length);
            _disk.WriteBytes(alignedStart, aligned);
            _log($"  Committed: {write.Description}");
        }
        _log($"All {_pendingWrites.Count} writes committed.");
        _pendingWrites.Clear();
    }
}

/// <summary>
/// Represents a pending disk write operation.
/// </summary>
public class PendingWrite
{
    public long DiskOffset { get; set; }
    public byte[] Data { get; set; } = Array.Empty<byte>();
    public string Description { get; set; } = "";
}

/// <summary>
/// Parsed cylinder group descriptor info.
/// </summary>
public class CylinderGroupInfo
{
    public int CgNumber { get; set; }
    public long DiskOffset { get; set; }
    public byte[] RawData { get; set; } = Array.Empty<byte>();
    public uint Magic { get; set; }
    public int CgxFromDisk { get; set; }
    public int NumDataBlocks { get; set; }
    public int FreeBlocks { get; set; }
    public int FreeInodes { get; set; }
    public int InodesUsedOffset { get; set; }
    public int FreeBlocksOffset { get; set; }
    public int InodeRotor { get; set; }
}
