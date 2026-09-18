using System.Security.Cryptography;
using PS3HddTool.Core.FileSystem;

namespace PS3HddTool.Core.Disk;

public sealed record Ps4Mount(Ps4PartitionSource Source, Ufs2FileSystem FileSystem,
    long IvOffset, bool ReversedKey) : IDisposable
{
    public static byte[] ParseKey(string text)
    {
        text = text.Trim();
        // HDDKEY also accepts a 32-byte key imported before opening the disk.
        foreach (string prefix in new[] { "EAPKEY:", "HDDKEY:" })
            if (text.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) text = text[prefix.Length..];
        text = string.Concat(text.Where(c => !char.IsWhiteSpace(c) && c != '-' && c != ':'));
        if (text.Length != 64) throw new FormatException("Import a 32-byte PS4 EAP HDD key, or enter its 64 hex characters.");
        return Convert.FromHexString(text);
    }

    public static Ps4Mount Open(IDiskSource disk, Ps4Partition partition, byte[] eapKey)
    {
        if (!partition.CanBrowse) throw new NotSupportedException(partition.Support);
        if (eapKey.Length != 32) throw new ArgumentException("PS4 EAP HDD key must be 32 bytes.");
        byte[] key = (byte[])eapKey.Clone();
        try
        {
            foreach (bool reversed in new[] { false, true })
            {
                if (reversed) { Array.Reverse(key, 0, 16); Array.Reverse(key, 16, 16); }
                // GPT slot number, not the position in the list of populated partitions.
                foreach (long iv in new[] { 0L, ((long)partition.Slot - 1) << 32 }.Distinct())
                {
                    var source = new Ps4PartitionSource(disk, partition, key, iv);
                    bool success = false;
                    try
                    {
                        var fs = new Ufs2FileSystem(source, 0);
                        if (!fs.Mount() || !fs.Superblock!.IsLittleEndian) continue;
                        var root = fs.ReadInode(2);
                        if (root.FileType != Ufs2FileType.Directory || root.Size is < 24 or > 16 * 1024 * 1024) continue;
                        var entries = fs.ReadDirectory(root);
                        if (!entries.Any(e => e.Name == "." && e.InodeNumber == 2) ||
                            !entries.Any(e => e.Name == ".." && e.InodeNumber == 2)) continue;
                        success = true;
                        return new Ps4Mount(source, fs, iv, reversed);
                    }
                    catch (InvalidDataException) { /* Wrong key/IV or unsupported geometry. */ }
                    finally { if (!success) source.Dispose(); }
                }
            }
        }
        finally { CryptographicOperations.ZeroMemory(key); }
        throw new InvalidDataException("Unable to mount this PS4 partition: verify the console's EAP HDD key and image integrity. Both key byte orders and IV modes were checked.");
    }

    public void Dispose() => Source.Dispose();
}
