using System.Buffers.Binary;
using System.Text;

namespace PS3HddTool.Core.FileSystem;

/// <summary>
/// Parser for PS3 PARAM.SFO files (System File Object).
/// Little-endian format used across PS3/PSP/Vita.
/// 
/// Structure:
///   Header (0x14 bytes): magic, version, key_table_start, data_table_start, entries_count
///   Index Table: entries_count × 0x10 bytes each
///   Key Table: null-terminated ASCII strings
///   Data Table: UTF-8 strings or int32 values
/// </summary>
public class ParamSfo
{
    public Dictionary<string, string> StringParams { get; } = new();
    public Dictionary<string, uint> IntParams { get; } = new();

    public string? Title => StringParams.GetValueOrDefault("TITLE");
    public string? TitleId => StringParams.GetValueOrDefault("TITLE_ID");
    public string? Category => StringParams.GetValueOrDefault("CATEGORY");
    public string? ContentId => StringParams.GetValueOrDefault("CONTENT_ID");
    public string? AppVer => StringParams.GetValueOrDefault("APP_VER");
    public string? Version => StringParams.GetValueOrDefault("VERSION");

    /// <summary>
    /// Parse a PARAM.SFO from raw bytes. Returns null if invalid.
    /// </summary>
    public static ParamSfo? Parse(byte[] data)
    {
        if (data == null || data.Length < 0x14) return null;

        // Header — little-endian!
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0x00));
        if (magic != 0x46535000) return null; // "\0PSF" in LE = 0x00505346 → wait, PSF = 0x50 0x53 0x46
        // Actually: bytes 0x00-0x03 = 00-50-53-46 = "\0PSF"
        // As uint32 LE: 0x46535000
        // Let's just check bytes
        if (data[0] != 0x00 || data[1] != 0x50 || data[2] != 0x53 || data[3] != 0x46)
            return null;

        uint version = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0x04));
        int keyTableStart = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(0x08));
        int dataTableStart = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(0x0C));
        int entriesCount = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(0x10));

        if (entriesCount < 0 || entriesCount > 256) return null;
        if (keyTableStart < 0x14 || dataTableStart < keyTableStart) return null;

        var sfo = new ParamSfo();

        // Parse index table entries (starting at 0x14, each 0x10 bytes)
        for (int i = 0; i < entriesCount; i++)
        {
            int indexOffset = 0x14 + (i * 0x10);
            if (indexOffset + 0x10 > data.Length) break;

            ushort keyOffset = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(indexOffset));
            ushort paramFmt = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(indexOffset + 2));
            int paramLen = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(indexOffset + 4));
            int paramMaxLen = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(indexOffset + 8));
            int dataOffset = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(indexOffset + 12));

            // Read key name from key table
            int keyStart = keyTableStart + keyOffset;
            if (keyStart >= data.Length) continue;
            int keyEnd = Array.IndexOf(data, (byte)0, keyStart);
            if (keyEnd < 0 || keyEnd > data.Length) keyEnd = Math.Min(keyStart + 64, data.Length);
            string keyName = Encoding.ASCII.GetString(data, keyStart, keyEnd - keyStart);

            // Read value from data table
            int dataStart = dataTableStart + dataOffset;
            if (dataStart >= data.Length) continue;

            switch (paramFmt)
            {
                case 0x0204: // UTF-8 string (null-terminated)
                case 0x0004: // UTF-8 special
                    if (dataStart + paramLen <= data.Length && paramLen > 0)
                    {
                        int strEnd = Array.IndexOf(data, (byte)0, dataStart, paramLen);
                        int strLen = strEnd >= 0 ? strEnd - dataStart : paramLen;
                        string value = Encoding.UTF8.GetString(data, dataStart, strLen);
                        sfo.StringParams[keyName] = value;
                    }
                    break;

                case 0x0404: // int32
                    if (dataStart + 4 <= data.Length)
                    {
                        uint intValue = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(dataStart));
                        sfo.IntParams[keyName] = intValue;
                    }
                    break;
            }
        }

        return sfo;
    }
}
