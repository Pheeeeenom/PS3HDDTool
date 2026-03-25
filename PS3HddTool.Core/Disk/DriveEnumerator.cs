using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace PS3HddTool.Core.Disk;

public class PhysicalDriveInfo
{
    public string Path { get; set; } = "";
    public long Size { get; set; }
    public string SizeFormatted { get; set; } = "";
    public string Model { get; set; } = "";
    public string DisplayName => string.IsNullOrEmpty(Model) 
        ? $"{Path} — {SizeFormatted}" 
        : $"{Path} — {Model} ({SizeFormatted})";
    
    public override string ToString() => DisplayName;
}

public static class DriveEnumerator
{
    /// <summary>
    /// Enumerate all physical drives on the system.
    /// Windows-only: probes \\.\PhysicalDrive0 through \\.\PhysicalDrive15
    /// </summary>
    public static List<PhysicalDriveInfo> EnumerateDrives()
    {
        var drives = new List<PhysicalDriveInfo>();

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            EnumerateWindows(drives);
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            EnumerateLinux(drives);
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            EnumerateMac(drives);
        }

        return drives;
    }

    private static void EnumerateWindows(List<PhysicalDriveInfo> drives)
    {
        for (int i = 0; i <= 15; i++)
        {
            string path = $"\\\\.\\PhysicalDrive{i}";
            try
            {
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 512, FileOptions.None);
                long size = DetectSize(fs);
                if (size > 0)
                {
                    drives.Add(new PhysicalDriveInfo
                    {
                        Path = path,
                        Size = size,
                        SizeFormatted = FormatSize(size),
                        Model = "" // WMI would give model but adds complexity
                    });
                }
            }
            catch
            {
                // Drive doesn't exist or no access
            }
        }
    }

    private static void EnumerateLinux(List<PhysicalDriveInfo> drives)
    {
        // Check /dev/sd[a-z] and /dev/nvme[0-9]n1
        foreach (char c in "abcdefghijklmnopqrstuvwxyz")
        {
            string path = $"/dev/sd{c}";
            TryAddDrive(drives, path);
        }
        for (int i = 0; i < 8; i++)
        {
            string path = $"/dev/nvme{i}n1";
            TryAddDrive(drives, path);
        }
    }

    private static void EnumerateMac(List<PhysicalDriveInfo> drives)
    {
        for (int i = 0; i < 10; i++)
        {
            string path = $"/dev/disk{i}";
            TryAddDrive(drives, path);
        }
    }

    private static void TryAddDrive(List<PhysicalDriveInfo> drives, string path)
    {
        try
        {
            if (!File.Exists(path)) return;
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 512);
            long size = DetectSize(fs);
            if (size > 0)
            {
                drives.Add(new PhysicalDriveInfo
                {
                    Path = path,
                    Size = size,
                    SizeFormatted = FormatSize(size)
                });
            }
        }
        catch { }
    }

    private static long DetectSize(FileStream stream)
    {
        // Try seek to end
        try
        {
            long size = stream.Seek(0, SeekOrigin.End);
            if (size > 0) return size;
        }
        catch { }

        // Binary search
        long lo = 0, hi = 8L * 1024 * 1024 * 1024 * 1024; // 8TB max
        byte[] buf = new byte[512];
        while (lo < hi - 512)
        {
            long mid = ((lo + hi) / 2 / 512) * 512;
            try
            {
                stream.Seek(mid, SeekOrigin.Begin);
                int read = stream.Read(buf, 0, 512);
                if (read > 0) lo = mid + 512;
                else hi = mid;
            }
            catch
            {
                hi = mid;
            }
        }
        return lo;
    }

    private static string FormatSize(long bytes)
    {
        if (bytes >= 1L << 40) return $"{bytes / (double)(1L << 40):F2} TB";
        if (bytes >= 1L << 30) return $"{bytes / (double)(1L << 30):F2} GB";
        if (bytes >= 1L << 20) return $"{bytes / (double)(1L << 20):F2} MB";
        return $"{bytes:N0} bytes";
    }
}
