using System.Buffers.Binary;

namespace PS3HddTool.Core.Disk;

public sealed record Ps4Partition(int Slot, Guid TypeGuid, long StartSector, long SectorCount)
{
    public string Name => TypeGuid.ToString() switch
    {
        "c638477a-e002-4b57-a454-a27fb63a33a8" => "user",
        "21e4dfb4-0040-4934-a037-ea9dc058eea6" => "eap_user",
        "6e0c5310-8445-4066-b571-9b65fdb75935" => "eap_vsh",
        "fdb5ede1-73c3-4c43-8c5b-2d3dcfcddff8" => "update",
        "17800f17-b9e1-425d-b937-0119a0813172" => "preinst",
        "ccb52e94-ebef-48c4-a195-9e2da5b0292c" => "preinst2",
        "145268bf-63ad-47c1-9378-9aacd9beed7c" => "eap_kern",
        "757a614b-6179-5361-6b61-6b6968617261" or
        "eabbf00b-c299-4488-9de9-b2839bce7546" => "system",
        "dc85025f-a694-4109-be44-fa0c063e8b81" => "system_ex",
        "76a9a5b4-44b0-472a-bde3-3107472adee2" => "swap",
        "80dd49e3-a985-4887-81de-1daca47aed90" => "app_tmp",
        "a71ff62d-1421-4dd9-935d-25dabd81bec5" => "system_data",
        "3ef7290a-de81-4887-a11f-46fba765c71c" => "app_reserved",
        _ => "Unknown partition"
    };
    public bool CanBrowse => Name is "user" or "eap_user";
    public string Support => CanBrowse ? "UFS2 · read-only" :
        Name is "update" or "eap_vsh" ? "FAT · not supported yet" : "Not supported with EAP HDD key";
    public string DisplayName => $"{Name} (GPT {Slot}) · {SectorCount * 512.0 / (1L << 30):F2} GiB · {Support}";
    public override string ToString() => DisplayName;
}

/// <summary>Reads the unencrypted GPT; preserves slot numbers used by PS4 XTS IVs.</summary>
public sealed record Ps4DiskLayout(Guid DiskId, IReadOnlyList<Ps4Partition> Partitions)
{
    public static Ps4DiskLayout? TryRead(IDiskSource disk)
    {
        if (disk.SectorSize != 512 || disk.TotalSize < 1024) return null;
        byte[] header = disk.ReadSectors(1, 1);
        if (!header.AsSpan(0, 8).SequenceEqual("EFI PART"u8)) return null;

        uint size = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(12));
        if (size < 92 || size > 512) throw new InvalidDataException("Invalid GPT header size.");
        uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16));
        header.AsSpan(16, 4).Clear();
        if (Crc32(header.AsSpan(0, (int)size)) != expectedCrc)
            throw new InvalidDataException("GPT header checksum failed. Use an intact full disk image.");

        ulong first = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(40));
        ulong last = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(48));
        ulong entryLba = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(72));
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(80));
        uint entrySize = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(84));
        if (first > last || last >= (ulong)disk.SectorCount || entryLba < 2 ||
            count is 0 or > 4096 || entrySize is < 128 or > 4096 || entrySize % 128 != 0)
            throw new InvalidDataException("Invalid or truncated GPT disk layout.");
        long tableBytes = (long)count * entrySize;
        if (entryLba > (ulong)(disk.TotalSize / 512) ||
            tableBytes > disk.TotalSize - (long)entryLba * 512)
            throw new InvalidDataException("GPT partition table extends past the image.");
        byte[] entries = disk.ReadBytes((long)entryLba * 512, (int)tableBytes);
        if (Crc32(entries) != BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(88)))
            throw new InvalidDataException("GPT partition table checksum failed.");

        var partitions = new List<Ps4Partition>();
        for (int i = 0; i < count; i++)
        {
            var entry = entries.AsSpan(i * (int)entrySize, (int)entrySize);
            Guid type = new(entry[..16]);
            if (type == Guid.Empty) continue;
            ulong start = BinaryPrimitives.ReadUInt64LittleEndian(entry[32..]);
            ulong end = BinaryPrimitives.ReadUInt64LittleEndian(entry[40..]);
            if (start < first || end < start || end > last)
                throw new InvalidDataException($"GPT partition {i + 1} is outside disk bounds.");
            partitions.Add(new Ps4Partition(i + 1, type, (long)start, (long)(end - start + 1)));
        }
        if (!partitions.Any(p => p.CanBrowse))
            throw new NotSupportedException("This GPT disk has no supported PS4 user/eap_user partitions.");
        return new Ps4DiskLayout(new Guid(header.AsSpan(56, 16)), partitions);
    }

    private static uint Crc32(ReadOnlySpan<byte> data)
    {
        uint crc = uint.MaxValue;
        foreach (byte value in data)
        {
            crc ^= value;
            for (int bit = 0; bit < 8; bit++)
                crc = (crc >> 1) ^ ((crc & 1) != 0 ? 0xEDB88320u : 0);
        }
        return ~crc;
    }
}
