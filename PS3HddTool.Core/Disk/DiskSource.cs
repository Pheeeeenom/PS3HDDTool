namespace PS3HddTool.Core.Disk;

/// <summary>
/// Abstraction for reading sectors from either a physical disk or an image file.
/// </summary>
public interface IDiskSource : IDisposable
{
    long TotalSize { get; }
    int SectorSize { get; }
    long SectorCount { get; }
    string Description { get; }
    byte[] ReadSectors(long startSector, int count);
    byte[] ReadBytes(long offset, int count);

    /// <summary>Write sectors to the disk. Not all implementations support this.</summary>
    void WriteSectors(long startSector, byte[] data);

    /// <summary>Write raw bytes at an arbitrary offset (must be sector-aligned for physical disks).</summary>
    void WriteBytes(long offset, byte[] data);

    /// <summary>Whether this source supports writing.</summary>
    bool CanWrite { get; }
}

/// <summary>
/// Reads sectors from a disk image file (.img, .bin, .iso, etc.)
/// </summary>
public sealed class ImageDiskSource : IDiskSource
{
    private readonly FileStream _stream;
    private readonly object _lock = new();

    public long TotalSize { get; }
    public int SectorSize => 512;
    public long SectorCount => TotalSize / SectorSize;
    public string Description { get; }
    public bool CanWrite => _stream.CanWrite;

    public ImageDiskSource(string filePath, bool writable = false)
    {
        if (!File.Exists(filePath))
            throw new FileNotFoundException("Disk image not found.", filePath);

        _stream = new FileStream(filePath, FileMode.Open, 
            writable ? FileAccess.ReadWrite : FileAccess.Read, FileShare.Read);
        TotalSize = _stream.Length;
        Description = $"Image: {Path.GetFileName(filePath)} ({FormatSize(TotalSize)})";
    }

    public byte[] ReadSectors(long startSector, int count)
    {
        long offset = startSector * SectorSize;
        int length = count * SectorSize;
        return ReadBytes(offset, length);
    }

    public byte[] ReadBytes(long offset, int count)
    {
        lock (_lock)
        {
            byte[] buffer = new byte[count];
            _stream.Seek(offset, SeekOrigin.Begin);
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = _stream.Read(buffer, totalRead, count - totalRead);
                if (read == 0) break;
                totalRead += read;
            }
            return buffer;
        }
    }

    public void WriteSectors(long startSector, byte[] data)
    {
        WriteBytes(startSector * SectorSize, data);
    }

    public void WriteBytes(long offset, byte[] data)
    {
        lock (_lock)
        {
            _stream.Seek(offset, SeekOrigin.Begin);
            _stream.Write(data, 0, data.Length);
            _stream.Flush();
        }
    }

    public void Dispose() => _stream.Dispose();

    private static string FormatSize(long bytes)
    {
        string[] units = { "B", "KB", "MB", "GB", "TB" };
        double size = bytes;
        int unitIndex = 0;
        while (size >= 1024 && unitIndex < units.Length - 1)
        {
            size /= 1024;
            unitIndex++;
        }
        return $"{size:F2} {units[unitIndex]}";
    }
}

/// <summary>
/// Reads sectors from a physical disk device.
/// On Windows: \\.\PhysicalDriveN
/// On Linux: /dev/sdX
/// On macOS: /dev/diskN
/// </summary>
public sealed class PhysicalDiskSource : IDiskSource
{
    private readonly FileStream _stream;
    private readonly object _lock = new();
    private readonly bool _writable;

    public long TotalSize { get; }
    public int SectorSize => 512;
    public long SectorCount => TotalSize / SectorSize;
    public string Description { get; }
    public bool CanWrite => _writable;

    public PhysicalDiskSource(string devicePath, long diskSize, bool writable = false)
    {
        _writable = writable;
        var access = writable ? FileAccess.ReadWrite : FileAccess.Read;
        _stream = new FileStream(devicePath, FileMode.Open, access, FileShare.None,
            512, FileOptions.None);
        TotalSize = diskSize > 0 ? diskSize : DetectDiskSize(_stream);
        Description = $"Physical: {devicePath} ({FormatSize(TotalSize)})";
    }

    public byte[] ReadSectors(long startSector, int count)
    {
        // Limit read size to avoid Windows physical drive I/O errors
        // Windows may reject reads larger than ~64KB on some physical drives
        const int MaxSectorsPerRead = 128; // 64KB

        long offset = startSector * SectorSize;
        int totalLength = count * SectorSize;
        byte[] buffer = new byte[totalLength];

        lock (_lock)
        {
            int bufferOffset = 0;
            int remaining = count;

            while (remaining > 0)
            {
                int chunk = Math.Min(remaining, MaxSectorsPerRead);
                int chunkBytes = chunk * SectorSize;
                long readOffset = offset + bufferOffset;

                // Use Position + Read instead of Seek for better compatibility
                _stream.Position = readOffset;

                int totalRead = 0;
                while (totalRead < chunkBytes)
                {
                    int read = _stream.Read(buffer, bufferOffset + totalRead, chunkBytes - totalRead);
                    if (read == 0) break;
                    totalRead += read;
                }

                bufferOffset += chunkBytes;
                remaining -= chunk;
            }
        }

        return buffer;
    }

    /// <summary>
    /// ReadBytes on physical drives must be sector-aligned on Windows.
    /// This method aligns the read to sector boundaries automatically.
    /// </summary>
    public byte[] ReadBytes(long offset, int count)
    {
        // Align to sector boundaries
        long alignedStart = (offset / SectorSize) * SectorSize;
        long alignedEnd = ((offset + count + SectorSize - 1) / SectorSize) * SectorSize;
        int alignedLength = (int)(alignedEnd - alignedStart);
        int startDelta = (int)(offset - alignedStart);

        lock (_lock)
        {
            byte[] alignedBuffer = new byte[alignedLength];
            _stream.Seek(alignedStart, SeekOrigin.Begin);
            int totalRead = 0;
            while (totalRead < alignedLength)
            {
                int read = _stream.Read(alignedBuffer, totalRead, alignedLength - totalRead);
                if (read == 0) break;
                totalRead += read;
            }

            // Extract the requested range
            byte[] result = new byte[count];
            Array.Copy(alignedBuffer, startDelta, result, 0, Math.Min(count, totalRead - startDelta));
            return result;
        }
    }

    private static long DetectDiskSize(FileStream stream)
    {
        // Try seeking to end to determine size
        try
        {
            return stream.Seek(0, SeekOrigin.End);
        }
        catch
        {
            // Fallback: binary search for disk end
            return BinarySearchDiskSize(stream);
        }
    }

    private static long BinarySearchDiskSize(FileStream stream)
    {
        long low = 0;
        long high = 4L * 1024 * 1024 * 1024 * 1024; // 4 TB max
        byte[] test = new byte[512];

        while (low < high - 512)
        {
            long mid = ((low + high) / 2 / 512) * 512;
            try
            {
                stream.Seek(mid, SeekOrigin.Begin);
                int read = stream.Read(test, 0, 512);
                if (read > 0) low = mid;
                else high = mid;
            }
            catch
            {
                high = mid;
            }
        }

        return low + 512;
    }

    public void WriteSectors(long startSector, byte[] data)
    {
        if (!_writable) throw new InvalidOperationException("Disk opened read-only.");
        if (data.Length % SectorSize != 0) throw new ArgumentException("Data must be sector-aligned.");
        lock (_lock)
        {
            _stream.Position = startSector * SectorSize;
            _stream.Write(data, 0, data.Length);
            _stream.Flush();
        }
    }

    public void WriteBytes(long offset, byte[] data)
    {
        if (!_writable) throw new InvalidOperationException("Disk opened read-only.");
        if (offset % SectorSize != 0 || data.Length % SectorSize != 0)
            throw new ArgumentException("Physical disk writes must be sector-aligned.");
        lock (_lock)
        {
            _stream.Position = offset;
            _stream.Write(data, 0, data.Length);
            _stream.Flush();
        }
    }

    public void Dispose() => _stream.Dispose();

    /// <summary>
    /// Detect logical and physical sector sizes for a drive.
    /// 4K native: logical=4096, physical=4096 — INCOMPATIBLE with PS3.
    /// 512e:      logical=512,  physical=4096 — OK.
    /// 512n:      logical=512,  physical=512  — OK.
    /// Returns (0, 0) if detection fails.
    /// </summary>
    public static (int LogicalSectorSize, int PhysicalSectorSize) DetectSectorSizes(string devicePath)
    {
        if (OperatingSystem.IsWindows())
            return DetectSectorSizesWindows(devicePath);
        if (OperatingSystem.IsLinux())
            return DetectSectorSizesLinux(devicePath);
        return (0, 0);
    }

    /// <summary>
    /// Returns true if the drive is 4K native (no 512-byte emulation).
    /// </summary>
    public static bool Is4KNative(string devicePath)
    {
        var (logical, _) = DetectSectorSizes(devicePath);
        return logical >= 4096;
    }

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static (int, int) DetectSectorSizesWindows(string devicePath)
    {
        try
        {
            const uint IOCTL_STORAGE_QUERY_PROPERTY = 0x002D1400;
            var handle = CreateFileW(devicePath, 0, 0x03, IntPtr.Zero, 3, 0, IntPtr.Zero);
            if (handle == IntPtr.Zero || handle == new IntPtr(-1))
                return (0, 0);
            try
            {
                byte[] query = new byte[12];
                BitConverter.TryWriteBytes(query.AsSpan(0), 6);
                BitConverter.TryWriteBytes(query.AsSpan(4), 0);
                byte[] output = new byte[64];
                bool ok = DeviceIoControl(handle, IOCTL_STORAGE_QUERY_PROPERTY,
                    query, query.Length, output, output.Length, out int bytesReturned, IntPtr.Zero);
                if (!ok || bytesReturned < 28) return (0, 0);
                return (BitConverter.ToInt32(output, 16), BitConverter.ToInt32(output, 20));
            }
            finally { CloseHandle(handle); }
        }
        catch { return (0, 0); }
    }

    [System.Runtime.Versioning.SupportedOSPlatform("linux")]
    private static (int, int) DetectSectorSizesLinux(string devicePath)
    {
        try
        {
            string devName = Path.GetFileName(devicePath);
            string logPath = $"/sys/block/{devName}/queue/logical_block_size";
            string physPath = $"/sys/block/{devName}/queue/physical_block_size";
            int logical = 0, physical = 0;
            if (File.Exists(logPath)) int.TryParse(File.ReadAllText(logPath).Trim(), out logical);
            if (File.Exists(physPath)) int.TryParse(File.ReadAllText(physPath).Trim(), out physical);
            return (logical, physical);
        }
        catch { return (0, 0); }
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true, CharSet = System.Runtime.InteropServices.CharSet.Unicode)]
    private static extern IntPtr CreateFileW(string lpFileName, uint dwDesiredAccess, uint dwShareMode,
        IntPtr lpSecurityAttributes, uint dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

    [System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool DeviceIoControl(IntPtr hDevice, uint dwIoControlCode,
        byte[] lpInBuffer, int nInBufferSize, byte[] lpOutBuffer, int nOutBufferSize,
        out int lpBytesReturned, IntPtr lpOverlapped);

    [System.Runtime.InteropServices.DllImport("kernel32.dll")]
    private static extern bool CloseHandle(IntPtr hObject);

    private static string FormatSize(long bytes)
    {
        string[] units = { "B", "KB", "MB", "GB", "TB" };
        double size = bytes;
        int unitIndex = 0;
        while (size >= 1024 && unitIndex < units.Length - 1)
        {
            size /= 1024;
            unitIndex++;
        }
        return $"{size:F2} {units[unitIndex]}";
    }
}
