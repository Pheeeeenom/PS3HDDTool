using System.Buffers.Binary;
using System.Text;
using PS3HddTool.Core.Disk;

internal sealed class MemoryDisk(byte[] bytes) : IDiskSource
{
    public byte[] Bytes { get; } = bytes;
    public long TotalSize => Bytes.Length;
    public int SectorSize => 512;
    public long SectorCount => TotalSize / 512;
    public string Description => "Synthetic disk";
    public bool CanWrite => true;
    public bool Disposed { get; private set; }
    public int Writes { get; private set; }
    public int MaxRead { get; private set; }
    public byte[] ReadSectors(long sector, int count) => ReadBytes(checked(sector * 512), checked(count * 512));
    public byte[] ReadBytes(long offset, int count)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (offset < 0 || count < 0 || offset > TotalSize - count) throw new EndOfStreamException();
        MaxRead = Math.Max(MaxRead, count);
        return Bytes.AsSpan((int)offset, count).ToArray();
    }
    public void WriteSectors(long sector, byte[] data) => WriteBytes(sector * 512, data);
    public void WriteBytes(long offset, byte[] data) => WriteBytes(offset, data.AsSpan());
    public void WriteBytes(long offset, ReadOnlySpan<byte> data) { Writes++; data.CopyTo(Bytes.AsSpan((int)offset)); }
    public void Dispose() => Disposed = true;
}

internal static class Fixtures
{
    public const int BlockSize = 4096;
    public const int FragmentSize = 512;
    public const long TripleBlock = 12L + 512 + 512 * 512;
    public static readonly Guid UserGuid = new("c638477a-e002-4b57-a454-a27fb63a33a8");
    public static readonly Guid EapUserGuid = new("21e4dfb4-0040-4934-a037-ea9dc058eea6");
    public static readonly byte[] Key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();

    public static byte[] Ufs(bool little)
    {
        var data = new byte[2 * 1024 * 1024];
        void I32(int offset, int value) => Put32(data, offset, value, little);
        void I64(int offset, long value) => Put64(data, offset, value, little);
        const int sb = 65536;
        I32(sb + 0x55C, 0x19540119);
        I32(sb + 0x30, BlockSize); I32(sb + 0x34, FragmentSize);
        I32(sb + 0x2C, 1); I32(sb + 0xB8, 32); I32(sb + 0xBC, data.Length / FragmentSize);
        I32(sb + 0x10, 160);
        I64(sb + (little ? 0x438 : 0x218), data.Length / FragmentSize);
        I64(sb + (little ? 0x440 : 0x220), data.Length / FragmentSize - 320);
        I64(sb + 0x3F0, 2); I64(sb + 0x3F8, 100); I64(sb + 0x400, 24); I64(sb + 0x408, 5);
        Encoding.UTF8.GetBytes("Fixture").CopyTo(data, sb + (little ? 0x2A8 : 0x480));
        Inode(data, 2, 0x41ED, 512, little, 192);
        Directory(data, 192, little, (2, "."), (2, ".."), (3, "sparse.bin"), (4, "triple.bin"), (5, "folder"));
        Inode(data, 3, 0x81A4, (12L + 512 + 2) * BlockSize + 123, little, 200);
        int node3 = InodeOffset(3);
        I64(node3 + 0x70 + 16, 208); // Hole at logical block 1.
        I64(node3 + 0xD0, 224);
        I64(node3 + 0xD8, 240);
        I64(224 * FragmentSize, 216);
        I64(224 * FragmentSize + 16, 232); // Hole inside the indirect block.
        I64(240 * FragmentSize, 248);
        I64(248 * FragmentSize, 256);
        foreach (int frag in new[] { 200, 208, 216, 232, 256 })
            data.AsSpan(frag * FragmentSize, BlockSize).Fill((byte)(frag / 8));
        Inode(data, 4, 0x81A4, TripleBlock * BlockSize + 37, little, 0);
        I64(InodeOffset(4) + 0xE0, 280);
        I64(280 * FragmentSize, 288); I64(288 * FragmentSize, 296); I64(296 * FragmentSize, 304);
        data.AsSpan(304 * FragmentSize, 37).Fill(0x7B);
        Inode(data, 5, 0x41ED, 512, little, 312);
        Directory(data, 312, little, (5, "."), (2, ".."), (6, "hello.txt"), (7, "link"));
        Inode(data, 6, 0x81A4, 1537, little, 320);
        for (int i = 0; i < 1537; i++) data[320 * FragmentSize + i] = (byte)(i % 251);
        Inode(data, 7, 0xA1FF, 0, little, 0);
        return data;
    }

    public static int InodeOffset(int number) => 160 * FragmentSize + number * 256;
    public static void Inode(byte[] data, int number, ushort mode, long size, bool little, long direct)
    {
        int at = InodeOffset(number);
        Put16(data, at, mode, little); Put16(data, at + 2, 1, little);
        Put32(data, at + 12, BlockSize, little); Put64(data, at + 16, size, little);
        Put64(data, at + 0x28, 1_600_000_000, little);
        Put64(data, at + 0x70, direct, little);
    }

    public static void Directory(byte[] data, int fragment, bool little, params (int Inode, string Name)[] entries)
    {
        int at = fragment * FragmentSize;
        int end = at + 512;
        for (int i = 0; i < entries.Length; i++)
        {
            byte[] name = Encoding.UTF8.GetBytes(entries[i].Name);
            int length = i == entries.Length - 1 ? end - at : (8 + name.Length + 1 + 3) & ~3;
            Put32(data, at, entries[i].Inode, little); Put16(data, at + 4, (ushort)length, little);
            data[at + 6] = 4; data[at + 7] = (byte)name.Length; name.CopyTo(data, at + 8);
            at += length;
        }
    }

    public static byte[] GptDisk(byte[] partitionBytes)
    {
        var disk = new byte[8 * 1024 * 1024];
        "EFI PART"u8.CopyTo(disk.AsSpan(512));
        Put32(disk, 512 + 8, 0x10000, true); Put32(disk, 512 + 12, 92, true);
        Put64(disk, 512 + 24, 1, true); Put64(disk, 512 + 32, disk.Length / 512 - 1, true);
        Put64(disk, 512 + 40, 34, true); Put64(disk, 512 + 48, disk.Length / 512 - 34, true);
        Guid.Parse("f06204d3-5d61-46b3-a04f-f79d9183b1c6").TryWriteBytes(disk.AsSpan(512 + 56));
        Put64(disk, 512 + 72, 2, true); Put32(disk, 512 + 80, 128, true); Put32(disk, 512 + 84, 128, true);
        AddPartition(disk, 27, 2048, partitionBytes.Length / 512, UserGuid);
        AddPartition(disk, 29, 8192, partitionBytes.Length / 512, EapUserGuid);
        partitionBytes.CopyTo(disk, 2048 * 512); partitionBytes.CopyTo(disk, 8192 * 512);
        UpdateCrc(disk);
        return disk;
    }
    public static void AddPartition(byte[] disk, int slot, long start, long count, Guid type)
    {
        int at = 1024 + (slot - 1) * 128;
        type.TryWriteBytes(disk.AsSpan(at));
        Put64(disk, at + 32, start, true); Put64(disk, at + 40, start + count - 1, true);
    }
    public static void UpdateCrc(byte[] disk)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(disk.AsSpan(512 + 88), Crc(disk.AsSpan(1024, 128 * 128)));
        disk.AsSpan(512 + 16, 4).Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(disk.AsSpan(512 + 16), Crc(disk.AsSpan(512, 92)));
    }
    private static uint Crc(ReadOnlySpan<byte> data)
    {
        uint value = uint.MaxValue;
        foreach (byte b in data)
        {
            value ^= b;
            for (int i = 0; i < 8; i++) value = (value & 1) == 0 ? value >> 1 : (value >> 1) ^ 0xEDB88320;
        }
        return value ^ uint.MaxValue;
    }
    public static void Put16(byte[] data, int at, ushort value, bool little)
    { if (little) BinaryPrimitives.WriteUInt16LittleEndian(data.AsSpan(at), value); else BinaryPrimitives.WriteUInt16BigEndian(data.AsSpan(at), value); }
    public static void Put32(byte[] data, int at, int value, bool little)
    { if (little) BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(at), value); else BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(at), value); }
    public static void Put64(byte[] data, int at, long value, bool little)
    { if (little) BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(at), value); else BinaryPrimitives.WriteInt64BigEndian(data.AsSpan(at), value); }
}

internal sealed class VerifySparseStream(long expectedSize, Dictionary<long, byte> blockValues) : Stream
{
    private long _position;
    public override void Write(byte[] buffer, int offset, int count)
    {
        if (_position + count > expectedSize || count > 4 * 1024 * 1024) throw new Exception("Unbounded or excessive extraction.");
        var span = buffer.AsSpan(offset, count);
        while (!span.IsEmpty)
        {
            long block = _position / Fixtures.BlockSize;
            int length = Math.Min(span.Length, Fixtures.BlockSize - (int)(_position % Fixtures.BlockSize));
            byte expected = blockValues.GetValueOrDefault(block);
            if (span[..length].IndexOfAnyExcept(expected) >= 0) throw new Exception($"Incorrect extracted data at byte {_position}.");
            _position += length;
            span = span[length..];
        }
    }
    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _position;
    public override long Position { get => _position; set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
