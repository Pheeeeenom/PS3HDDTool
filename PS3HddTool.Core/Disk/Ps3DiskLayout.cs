using System.Buffers.Binary;
using System.Text;

namespace PS3HddTool.Core.Disk;

/// <summary>
/// Represents the PS3 HDD layout.
/// The PS3 HDD has an encrypted partition table with VFLASH regions.
/// The main data region uses UFS2 filesystem.
/// </summary>
public class Ps3DiskLayout
{
    public Ps3HddHeader? Header { get; set; }
    public List<Ps3Partition> Partitions { get; set; } = new();
    public long DataRegionStartSector { get; set; }
    public long DataRegionSectorCount { get; set; }
}

/// <summary>
/// PS3 HDD header located at the beginning of the disk.
/// Contains magic bytes, version info, and encrypted key data.
/// </summary>
public class Ps3HddHeader
{
    public const uint ExpectedMagic = 0x504D4100; // "PMA\0" — PS3 partition magic
    public const int HeaderSize = 512;

    public uint Magic { get; set; }
    public uint Version { get; set; }
    public ulong DiskSectors { get; set; }
    public byte[] EncryptedAtaKeys { get; set; } = Array.Empty<byte>();
    public bool IsValid => Magic == ExpectedMagic;

    /// <summary>
    /// Parse the PS3 HDD header from a raw 512-byte sector.
    /// </summary>
    public static Ps3HddHeader Parse(byte[] data)
    {
        if (data.Length < HeaderSize)
            throw new ArgumentException($"Header data must be at least {HeaderSize} bytes.");

        var header = new Ps3HddHeader
        {
            Magic = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(0)),
            Version = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(4)),
            DiskSectors = BinaryPrimitives.ReadUInt64BigEndian(data.AsSpan(8)),
            EncryptedAtaKeys = data[16..80] // 64 bytes of encrypted ATA key data
        };

        return header;
    }
}

/// <summary>
/// Represents a partition on the PS3 HDD.
/// </summary>
public class Ps3Partition
{
    public int Index { get; set; }
    public string Name { get; set; } = "";
    public long StartSector { get; set; }
    public long SectorCount { get; set; }
    public Ps3PartitionType Type { get; set; }
    public long SizeBytes => SectorCount * 512;

    public string SizeFormatted
    {
        get
        {
            string[] units = { "B", "KB", "MB", "GB", "TB" };
            double size = SizeBytes;
            int i = 0;
            while (size >= 1024 && i < units.Length - 1) { size /= 1024; i++; }
            return $"{size:F2} {units[i]}";
        }
    }
}

public enum Ps3PartitionType
{
    Unknown,
    VFlash,
    GameOS,      // The main UFS2 partition with game data
    OtherOS,     // Linux/OtherOS partition (older firmware)
    Swap,
    System
}

/// <summary>
/// Parses the PS3 disk layout, detecting partitions and regions.
/// </summary>
public static class Ps3DiskParser
{
    /// <summary>
    /// Known sector offsets for standard PS3 HDD layouts.
    /// The PS3 uses a fixed partition scheme rather than a dynamic table.
    /// </summary>
    private static readonly (string Name, long StartSector, Ps3PartitionType Type)[] KnownPartitions =
    {
        ("VFLASH Region 1", 0x00, Ps3PartitionType.VFlash),
        ("System", 0x800, Ps3PartitionType.System),
    };

    /// <summary>
    /// Parse the PS3 disk layout from decrypted header data.
    /// </summary>
    public static Ps3DiskLayout ParseLayout(IDiskSource disk, byte[] decryptedHeader)
    {
        var layout = new Ps3DiskLayout
        {
            Header = Ps3HddHeader.Parse(decryptedHeader)
        };

        // The PS3 GameOS (UFS2) partition typically starts at a fixed offset.
        // For a standard PS3 HDD, the main data partition starts after the
        // system area. The exact offset depends on firmware version.
        // Common layout:
        //   Sector 0:          Header / partition magic
        //   Sector 0x800:      System region
        //   Sector 0x2000:     VFLASH region  
        //   Sector ~0x8000+:   GameOS UFS2 data region (bulk of disk)

        // Try to detect the UFS2 superblock by scanning for the magic number
        // UFS2 superblock magic: 0x19540119 at offset 0x55C within the superblock
        // The superblock is typically at byte offset 65536 (sector 128) within the partition

        long diskSectors = disk.TotalSize / 512;

        // Scan known candidate start sectors for the GameOS partition
        long[] candidateStarts = { 0x2000, 0x4000, 0x8000, 0x10000, 0x20000 };

        foreach (long candidate in candidateStarts)
        {
            if (candidate + 256 >= diskSectors) continue;

            try
            {
                // UFS2 superblock is at byte offset 65536 (0x10000) from partition start
                // That's 128 sectors in
                long sbSector = candidate + 128;
                if (sbSector + 1 >= diskSectors) continue;

                // We'll check for the superblock later after decryption
                // For now, record this as a candidate partition
                layout.Partitions.Add(new Ps3Partition
                {
                    Index = layout.Partitions.Count,
                    Name = "GameOS (UFS2)",
                    StartSector = candidate,
                    SectorCount = diskSectors - candidate,
                    Type = Ps3PartitionType.GameOS
                });
                break;
            }
            catch
            {
                continue;
            }
        }

        // Add system partition
        layout.Partitions.Insert(0, new Ps3Partition
        {
            Index = 0,
            Name = "System",
            StartSector = 0,
            SectorCount = layout.Partitions.Count > 0
                ? layout.Partitions[0].StartSector
                : Math.Min(0x2000, diskSectors),
            Type = Ps3PartitionType.System
        });

        // Update indices
        for (int i = 0; i < layout.Partitions.Count; i++)
            layout.Partitions[i].Index = i;

        if (layout.Partitions.Any(p => p.Type == Ps3PartitionType.GameOS))
        {
            var gameOs = layout.Partitions.First(p => p.Type == Ps3PartitionType.GameOS);
            layout.DataRegionStartSector = gameOs.StartSector;
            layout.DataRegionSectorCount = gameOs.SectorCount;
        }

        return layout;
    }

    /// <summary>
    /// Scan decrypted data for a UFS2 superblock magic number.
    /// Returns the byte offset of the superblock if found, or -1.
    /// </summary>
    public static long FindUfs2Superblock(byte[] data)
    {
        // UFS2 magic: 0x19540119 (big-endian)
        // Located at offset 0x55C (1372) within the superblock structure
        byte[] magic = { 0x19, 0x54, 0x01, 0x19 };

        for (int i = 0; i <= data.Length - 4; i++)
        {
            if (data[i] == magic[0] && data[i + 1] == magic[1] &&
                data[i + 2] == magic[2] && data[i + 3] == magic[3])
            {
                return i;
            }
        }

        return -1;
    }
}
