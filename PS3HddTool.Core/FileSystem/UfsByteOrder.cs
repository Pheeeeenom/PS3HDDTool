using System.Buffers.Binary;

namespace PS3HddTool.Core.FileSystem;

internal readonly struct UfsByteOrder(bool littleEndian)
{
    public ushort ReadUInt16(ReadOnlySpan<byte> data) => littleEndian ? BinaryPrimitives.ReadUInt16LittleEndian(data) : BinaryPrimitives.ReadUInt16BigEndian(data);
    public short ReadInt16(ReadOnlySpan<byte> data) => littleEndian ? BinaryPrimitives.ReadInt16LittleEndian(data) : BinaryPrimitives.ReadInt16BigEndian(data);
    public uint ReadUInt32(ReadOnlySpan<byte> data) => littleEndian ? BinaryPrimitives.ReadUInt32LittleEndian(data) : BinaryPrimitives.ReadUInt32BigEndian(data);
    public int ReadInt32(ReadOnlySpan<byte> data) => littleEndian ? BinaryPrimitives.ReadInt32LittleEndian(data) : BinaryPrimitives.ReadInt32BigEndian(data);
    public long ReadInt64(ReadOnlySpan<byte> data) => littleEndian ? BinaryPrimitives.ReadInt64LittleEndian(data) : BinaryPrimitives.ReadInt64BigEndian(data);
}
