using System.Buffers.Binary;
using System.Collections.ObjectModel;
using System.Numerics;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using PS3HddTool.Core.Crypto;
using PS3HddTool.Core.Disk;
using PS3HddTool.Core.FileSystem;
using PS3HddTool.Core.Models;

namespace PS3HddTool.Avalonia.ViewModels;

public partial class MainViewModel : ObservableObject
{
    private IDiskSource? _diskSource;
    private DecryptedDiskSource? _decryptedSource;
    private Ufs2FileSystem? _fileSystem;
    private Ps3DiskLayout? _diskLayout;

    // Stored for reopening with write access
    private string? _physicalDrivePath;
    private long _physicalDriveSize;
    private byte[]? _cbcKey;
    private bool _cbcBswap;
    private long _partitionSector;
    private byte[]? _xtsDataKey;
    private byte[]? _xtsTweakKey;
    private bool _xtsBswap;
    private bool _isXts; // true = XTS (Slim/NOR), false = CBC (Fat NAND)
    public string DetectedEncryptionType => _isXts ? "XTS-128" : IsDecrypted ? "CBC-192" : "";
    public string EncryptionHint { get; set; } = ""; // Set by key database to skip wrong scan

    [ObservableProperty] private string _statusText = "Ready — Open a disk image or select a physical drive.";
    [ObservableProperty] private string _eidRootKeyHex = "";
    [ObservableProperty] private bool _isDiskOpen;
    [ObservableProperty] private bool _isDecrypted;
    [ObservableProperty] private bool _isFilesystemMounted;
    [ObservableProperty] private bool _isBusy;
    [ObservableProperty] private double _progressValue;
    [ObservableProperty] private string _progressText = "";
    [ObservableProperty] private bool _isProgressIndeterminate;
    [ObservableProperty] private DiskInfo? _diskInfo;
    [ObservableProperty] private FileTreeNode? _selectedNode;
    [ObservableProperty] private global::Avalonia.Media.Imaging.Bitmap? _imagePreview;
    [ObservableProperty] private bool _hasImagePreview;

    private static readonly HashSet<string> ImageExtensions = new(StringComparer.OrdinalIgnoreCase)
        { ".png", ".jpg", ".jpeg", ".bmp", ".gif" };

    partial void OnSelectedNodeChanged(FileTreeNode? value)
    {
        if (value != null && value.IsDirectory && !value.ChildrenLoaded)
        {
            _ = ExpandNodeAsync(value);
        }

        // Load image preview if it's an image file
        if (value != null && !value.IsDirectory && ImageExtensions.Contains(Path.GetExtension(value.Name)))
        {
            _ = LoadImagePreviewAsync(value);
        }
        else
        {
            ImagePreview = null;
            HasImagePreview = false;
        }
    }

    private async Task LoadImagePreviewAsync(FileTreeNode node)
    {
        if (_fileSystem == null) return;

        try
        {
            // Only preview files under 10MB to avoid memory issues
            if (node.Size > 10 * 1024 * 1024)
            {
                HasImagePreview = false;
                return;
            }

            byte[]? imageData = null;
            await Task.Run(() =>
            {
                var inode = _fileSystem.ReadInode(node.InodeNumber);
                imageData = _fileSystem.ReadInodeData(inode);
            });

            if (imageData != null && imageData.Length > 0)
            {
                using var ms = new MemoryStream(imageData);
                ImagePreview = new global::Avalonia.Media.Imaging.Bitmap(ms);
                HasImagePreview = true;
            }
        }
        catch
        {
            ImagePreview = null;
            HasImagePreview = false;
        }
    }

    public ObservableCollection<FileTreeNode> FileTree { get; } = new();
    public ObservableCollection<Ps3Partition> Partitions { get; } = new();
    public ObservableCollection<string> LogMessages { get; } = new();

    /// <summary>
    /// Open a disk image file.
    /// </summary>
    [RelayCommand]
    public async Task OpenImageAsync(string filePath)
    {
        try
        {
            IsBusy = true;
            StatusText = $"Opening {Path.GetFileName(filePath)}...";
            Log($"Opening image: {filePath}");

            await Task.Run(() =>
            {
                _diskSource?.Dispose();
                _decryptedSource?.Dispose();

                _diskSource = new ImageDiskSource(filePath);
            });

            IsDiskOpen = true;
            IsDecrypted = false;
            IsFilesystemMounted = false;
            FileTree.Clear();
            Partitions.Clear();

            DiskInfo = new DiskInfo
            {
                Source = _diskSource!.Description,
                TotalSize = _diskSource.TotalSize,
                TotalSizeFormatted = FormatSize(_diskSource.TotalSize),
                IsEncrypted = true,
                Status = "Disk opened — enter EID Root Key and decrypt."
            };

            StatusText = $"Disk image opened: {FormatSize(_diskSource.TotalSize)}. Enter EID Root Key to decrypt.";
            Log($"Image loaded: {_diskSource.SectorCount} sectors, {FormatSize(_diskSource.TotalSize)}");
        }
        catch (Exception ex)
        {
            StatusText = $"Error: {ex.Message}";
            Log($"ERROR: {ex.Message}");
        }
        finally
        {
            IsBusy = false;
        }
    }

    /// <summary>
    /// Open a physical drive.
    /// </summary>
    [RelayCommand]
    public async Task OpenPhysicalDriveAsync((string Path, long Size) drive)
    {
        try
        {
            IsBusy = true;
            StatusText = $"Opening {drive.Path}...";
            Log($"Opening physical drive: {drive.Path}");

            await Task.Run(() =>
            {
                _diskSource?.Dispose();
                _decryptedSource?.Dispose();

                _diskSource = new PhysicalDiskSource(drive.Path, drive.Size, writable: true);
                _physicalDrivePath = drive.Path;
                _physicalDriveSize = drive.Size;
            });

            IsDiskOpen = true;
            IsDecrypted = false;
            IsFilesystemMounted = false;
            FileTree.Clear();
            Partitions.Clear();

            DiskInfo = new DiskInfo
            {
                Source = _diskSource!.Description,
                TotalSize = _diskSource.TotalSize,
                TotalSizeFormatted = FormatSize(_diskSource.TotalSize),
                IsEncrypted = true,
                Status = "Drive opened — enter EID Root Key and decrypt."
            };

            StatusText = $"Physical drive opened. Enter EID Root Key to decrypt.";
            Log($"Drive opened: {_diskSource.SectorCount} sectors, {FormatSize(_diskSource.TotalSize)}");
        }
        catch (Exception ex)
        {
            StatusText = $"Error: {ex.Message}";
            Log($"ERROR: {ex.Message}");
        }
        finally
        {
            IsBusy = false;
        }
    }

    /// <summary>
    /// Candidate partition start sectors to scan for UFS2 superblock.
    /// Covers known PS3 layouts across firmware versions.
    /// </summary>
    private static readonly long[] CandidatePartitionStarts =
    {
        0x400018, 0x400020, 0x400000, 0x3FFFF8, // dev_hdd0 (after ~2GB dev_hdd1)
        // Slim/NOR: VFLASH=0x80000 sectors, GameOS starts after VFLASH + padding
        0x80018, 0x80020, 0x80028, 0x80010, 0x80008, 0x80030,
        0, 2, 8, 16, 0x20, 0x22, 0x28, 0x30, 0x40, 0x80,
        128, 256, 512, 1024, 
        0x800, 0x1000, 0x2000, 0x4000, 0x8000,
        0x10000, 0x20000, 
        0x40000, 0x80000,
        0x100000, 0x200000, 0x400000, 0x800000,
    };

    /// <summary>
    /// Decrypt the disk using the provided EID Root Key.
    /// Tries all known key derivation methods and scans for UFS2 superblock.
    /// </summary>
    [RelayCommand]
    public async Task DecryptAsync()
    {
        if (_diskSource == null || string.IsNullOrWhiteSpace(EidRootKeyHex))
        {
            StatusText = "Please open a disk and enter the EID Root Key first.";
            return;
        }

        try
        {
            IsBusy = true;
            IsProgressIndeterminate = true;
            StatusText = "Parsing EID Root Key...";

            // Start fresh log section
            LogSeparator();
            Log("PS3 HDD Tool — Decryption Attempt");
            LogSeparator();

            // Run XTS self-test first
            bool xtsOk = AesXts128.SelfTest();
            Log($"AES-XTS-128 self-test: {(xtsOk ? "PASSED" : "FAILED")}");
            if (!xtsOk)
            {
                StatusText = "FATAL: AES-XTS implementation failed self-test!";
                return;
            }

            // Verify optimized CBC-192 encrypt matches original
            {
                byte[] testKey = new byte[24];
                Array.Fill(testKey, (byte)0xAB);
                byte[] testData = new byte[1024 * 1024];
                new Random(42).NextBytes(testData);
                using var cbc = new AesCbc192(testKey);
                byte[] enc1 = cbc.EncryptSectors(testData);
                byte[] enc2 = cbc.EncryptSectorsOriginal(testData);
                bool cbcOk = enc1.AsSpan().SequenceEqual(enc2);
                Log($"AES-CBC-192 optimize self-test: {(cbcOk ? "PASSED" : "FAILED")} (1MB / {testData.Length / 512} sectors)");
                if (!cbcOk)
                {
                    StatusText = "FATAL: Optimized CBC-192 encryption produces different output!";
                    return;
                }
            }

            Log($"Disk: {_diskSource.Description}");
            Log($"Disk size: {_diskSource.TotalSize} bytes ({FormatSize(_diskSource.TotalSize)})");
            Log($"Sector count: {_diskSource.SectorCount}");

            byte[] eidRootKey;
            try
            {
                eidRootKey = Ps3KeyDerivation.ParseEidRootKey(EidRootKeyHex);
            }
            catch (Exception ex)
            {
                StatusText = $"Invalid EID Root Key: {ex.Message}";
                Log($"Key parse error: {ex.Message}");
                return;
            }

            Log($"EID Root Key accepted: {Ps3KeyDerivation.DescribeKey(eidRootKey)}");
            Log($"  ERK key (bytes 0-31):  {BitConverter.ToString(eidRootKey[..32])}");
            Log($"  ERK IV  (bytes 32-47): {BitConverter.ToString(eidRootKey[32..48])}");

            // Dump raw sector 0 before any decryption
            byte[] rawSector0 = _diskSource.ReadSectors(0, 1);
            Log($"Raw sector 0 (first 64 bytes): {BitConverter.ToString(rawSector0[..64])}");

            StatusText = "Deriving all possible key combinations...";

            // Get all possible key derivation results
            var allKeys = Ps3KeyDerivation.DeriveAllPossibleKeys(eidRootKey);

            // Log all derived keys
            foreach (var (method, dk, tk) in allKeys)
            {
                Log($"  Key set [{method}]:");
                Log($"    Data key:  {BitConverter.ToString(dk)}");
                Log($"    Tweak key: {BitConverter.ToString(tk)}");
            }

            // Also derive the 24-byte CBC-192 key for Fat NAND models
            var cbcKeys = Ps3KeyDerivation.DeriveKeysFatNand(eidRootKey);
            Log($"  CBC-192 ATA data key (24B): {BitConverter.ToString(cbcKeys.AtaDataKey)}");

            Log($"Will try CBC-192 (NAND) + {allKeys.Count} XTS key method(s) x 2 bswap modes x {CandidatePartitionStarts.Length} partition offsets...");

            bool found = false;
            string foundMethod = "";
            long foundPartitionSector = 0;
            bool foundBswap = false;
            bool foundCbc = false;

            await Task.Run(() =>
            {
                // ─── FAST PATH: Try known CECHA config first (CBC-192 + bswap16 + sector 0x20) ───
                if (EncryptionHint != "XTS-128")
                {
                Log("Trying fast path: CBC-192 + bswap16 + partition sector 0x20...");
                try
                {
                    var fastCandidate = new DecryptedDiskSourceCbc(
                        new NonDisposingDiskSource(_diskSource), cbcKeys.AtaDataKey, true);
                    
                    byte[] fastSec0 = fastCandidate.ReadSectors(0, 1);
                    uint fm1 = (uint)((fastSec0[0x14] << 24) | (fastSec0[0x15] << 16) |
                                       (fastSec0[0x16] << 8) | fastSec0[0x17]);
                    uint fm2 = (uint)((fastSec0[0x1C] << 24) | (fastSec0[0x1D] << 16) |
                                       (fastSec0[0x1E] << 8) | fastSec0[0x1F]);
                    
                    if (fm1 == 0x0FACE0FF && fm2 == 0xDEADFACE)
                    {
                        // Partition table valid! Check UFS2 at sector 0x20
                        long sbOff = (0x20 * 512) + 65536;
                        byte[] sbData = fastCandidate.ReadBytes(sbOff, 8192);
                        uint sbMagic = (uint)((sbData[0x55C] << 24) | (sbData[0x55D] << 16) |
                                               (sbData[0x55E] << 8) | sbData[0x55F]);
                        
                        if (sbMagic == 0x19540119)
                        {
                            found = true;
                            foundMethod = "AES-CBC-192 NAND [bswap16=True] (fast path)";
                            foundPartitionSector = 0x20;
                            foundBswap = true;
                            foundCbc = true;
                            Log("  *** FAST PATH SUCCESS: CECHA NAND detected! ***");
                        }
                    }
                    fastCandidate.Dispose();
                }
                catch (Exception ex)
                {
                    Log($"  Fast path failed: {ex.Message}");
                }
                } // end if (hint != XTS)
                else
                {
                    Log("Skipping CBC fast path (hint: XTS-128).");
                }

                if (found)
                {
                    // Skip the full scan — log what we found
                    Log($"SUCCESS: UFS2 superblock found via fast path!");
                    Log($"  Key method: {foundMethod}");
                    Log($"  Partition start: sector 0x{foundPartitionSector:X} ({foundPartitionSector})");
                }
                else
                {
                    Log("Fast path didn't match, falling back to full scan...");
                }

                // ─── FULL SCAN: Try all combinations (only if fast path failed) ───
                if (!found)
                {

                bool skipCbc = EncryptionHint == "XTS-128";
                bool skipXts = EncryptionHint == "CBC-192";

                if (skipCbc)
                    Log("  Hint: XTS-128 — skipping CBC-192 scan.");
                if (skipXts)
                    Log("  Hint: CBC-192 — skipping XTS-128 scan.");

                // ─── Try CBC-192 first (Fat NAND models like CECHA/B/C/E) ───
                if (!skipCbc)
                foreach (bool useBswap in new[] { false, true })
                {
                    if (found) break;

                    string label = $"AES-CBC-192 NAND [bswap16={useBswap}]";
                    var candidate = new DecryptedDiskSourceCbc(
                        new NonDisposingDiskSource(_diskSource), cbcKeys.AtaDataKey, useBswap);

                    try
                    {
                        byte[] sector0 = candidate.ReadSectors(0, 1);
                        uint magic1 = (uint)((sector0[0x14] << 24) | (sector0[0x15] << 16) |
                                              (sector0[0x16] << 8) | sector0[0x17]);
                        uint magic2 = (uint)((sector0[0x1C] << 24) | (sector0[0x1D] << 16) |
                                              (sector0[0x1E] << 8) | sector0[0x1F]);

                        bool valid = (magic1 == 0x0FACE0FF || magic2 == 0xDEADFACE);
                        Log($"  [{label}] Sector 0 magic: {magic1:X8} / {magic2:X8}" +
                            (valid ? " *** MATCH! ***" : " (expected 0FACE0FF/DEADFACE)"));

                        // Also log first 32 bytes for debug
                        Log($"    First 32 bytes: {BitConverter.ToString(sector0, 0, 32)}");

                        // If we found a valid partition table, parse it to find partition offsets
                        if (valid)
                        {
                            // PS3 partition table format (big-endian):
                            // Offset 0x20: number of partitions (8 bytes)
                            // ... then partition entries at 0x28+
                            // Each entry: 8 bytes start sector, 8 bytes sector count
                            // But the format varies. Let's read more sectors and log them.
                            byte[] header = candidate.ReadSectors(0, 4);
                            Log($"    Decrypted sectors 0-3 (first 256 bytes):");
                            for (int row = 0; row < 256; row += 32)
                                Log($"      {row:X4}: {BitConverter.ToString(header, row, 32)}");

                            // Parse partition entries from the PS3 disk header
                            // Based on psdevwiki: the partition table starts at 0x20
                            // Format: 8-byte num_regions, then each region has:
                            //   8-byte start_sector, 8-byte sector_count, ...
                            long numPartitions = ReadBE64(header, 0x20);
                            Log($"    Number of partitions: {numPartitions}");

                            // Read partition entries — they follow the header
                            // The structure has entries at offsets 0x28 + (i * entrySize)
                            // Entry size appears to be 0x18 (24 bytes) or variable
                            // Let's try common entry layouts
                            List<long> partitionStarts = new();
                            for (int pi = 0; pi < Math.Min(numPartitions, 16); pi++)
                            {
                                // Try entry at 0x40 + (pi * 0xC0) based on known layouts
                                int entryBase = 0x40 + (pi * 0xC0);
                                if (entryBase + 16 > header.Length) break;

                                long pStart = ReadBE64(header, entryBase);
                                long pCount = ReadBE64(header, entryBase + 8);

                                if (pStart > 0 && pStart < _diskSource.SectorCount && pCount > 0)
                                {
                                    Log($"    Partition {pi}: start=0x{pStart:X} ({pStart}), count=0x{pCount:X} ({pCount})");
                                    partitionStarts.Add(pStart);
                                }
                            }

                            // Also try simpler layout: entries at 0x28 + (i * 0x10)
                            if (partitionStarts.Count == 0)
                            {
                                for (int pi = 0; pi < Math.Min(numPartitions, 16); pi++)
                                {
                                    int entryBase = 0x28 + (pi * 0x10);
                                    if (entryBase + 16 > header.Length) break;

                                    long pStart = ReadBE64(header, entryBase);
                                    long pCount = ReadBE64(header, entryBase + 8);

                                    if (pStart > 0 && pStart < _diskSource.SectorCount && pCount > 0)
                                    {
                                        Log($"    Partition {pi} (alt): start=0x{pStart:X} ({pStart}), count=0x{pCount:X} ({pCount})");
                                        partitionStarts.Add(pStart);
                                    }
                                }
                            }

                            // Scan all discovered partition starts for UFS2 superblock
                            // Find ALL UFS2 superblocks and pick the largest partition
                            var allStarts = new HashSet<long>(partitionStarts);
                            foreach (var cs in CandidatePartitionStarts) allStarts.Add(cs);

                            long bestPartStart = -1;
                            long bestCgCount = 0;

                            // Fine scan: every 8 sectors in first 2GB looking for UFS2
                            // The kpartx NOR example showed dev_hdd0 at sector 0x80010
                            // For NAND, it could be at a similar offset
                            Log($"    Fine-scanning first 2GB (every 8 sectors) for UFS2...");
                            int foundCount = 0;
                            for (long scanSec = 0; scanSec < 0x400000 && foundCount < 20; scanSec += 8)
                            {
                                long scanOff = (scanSec * 512) + 65536;
                                if (scanOff + 8192 > _diskSource.TotalSize) continue;
                                try
                                {
                                    byte[] scanData = candidate.ReadBytes(scanOff, 8192);
                                    uint scanMagic = (uint)((scanData[0x55C] << 24) | (scanData[0x55D] << 16) |
                                                             (scanData[0x55E] << 8) | scanData[0x55F]);
                                    if (scanMagic == 0x19540119)
                                    {
                                        int ncg = (scanData[0xBC] << 24) | (scanData[0xBD] << 16) |
                                                  (scanData[0xBE] << 8) | scanData[0xBF];
                                        Log($"    *** UFS2 at sector 0x{scanSec:X} ({ncg} CGs) ***");
                                        foundCount++;
                                    }
                                }
                                catch { }
                            }
                            Log($"    Fine scan done. Found {foundCount} UFS2 superblock(s) in first 2GB.");

                            foreach (long ps in allStarts)
                            {
                                long sbOff = (ps * 512) + 65536;
                                if (sbOff + 8192 > _diskSource.TotalSize) continue;

                                try
                                {
                                    byte[] sbData = candidate.ReadBytes(sbOff, 8192);
                                    if (sbData.Length > 0x560)
                                    {
                                        uint sbMagic = (uint)((sbData[0x55C] << 24) | (sbData[0x55D] << 16) |
                                                               (sbData[0x55E] << 8) | sbData[0x55F]);

                                        if (sbMagic == 0x19540119)
                                        {
                                            // Read CG count to determine partition size
                                            int ncg = (sbData[0xBC] << 24) | (sbData[0xBD] << 16) |
                                                      (sbData[0xBE] << 8) | sbData[0xBF];
                                            Log($"    UFS2 found at sector 0x{ps:X}: {ncg} cylinder groups");

                                            if (ncg > bestCgCount)
                                            {
                                                bestCgCount = ncg;
                                                bestPartStart = ps;
                                            }
                                        }
                                    }
                                }
                                catch { }
                            }

                            if (bestPartStart >= 0)
                            {
                                found = true;
                                foundMethod = label;
                                foundPartitionSector = bestPartStart;
                                foundBswap = useBswap;
                                foundCbc = true;
                                Log($"    Selected partition at sector 0x{bestPartStart:X} ({bestCgCount} CGs) as largest");
                            }

                            if (!found)
                            {
                                Log("    UFS2 superblock not found at any parsed partition offset.");
                                Log("    Doing broad sector scan...");
                                // Broad scan: check every 0x8000 sectors (every ~16MB)
                                for (long s = 0; s < Math.Min(_diskSource.SectorCount, 0x10000000L) && !found; s += 0x8000)
                                {
                                    long sbOff = (s * 512) + 65536;
                                    if (sbOff + 8192 > _diskSource.TotalSize) continue;

                                    try
                                    {
                                        byte[] sbData = candidate.ReadBytes(sbOff, 8192);
                                        if (sbData.Length > 0x560)
                                        {
                                            uint sbMagic = (uint)((sbData[0x55C] << 24) | (sbData[0x55D] << 16) |
                                                                   (sbData[0x55E] << 8) | sbData[0x55F]);
                                            if (sbMagic == 0x19540119)
                                            {
                                                found = true;
                                                foundMethod = label;
                                                foundPartitionSector = s;
                                                foundBswap = useBswap;
                                                foundCbc = true;
                                                Log($"    UFS2 found via broad scan at sector 0x{s:X}!");
                                                break;
                                            }
                                        }
                                    }
                                    catch { }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Log($"  [{label}] Sector 0 error: {ex.Message}");
                    }

                    if (!found)
                        candidate.Dispose();
                }

                // ─── Then try XTS-128 (NOR/Slim models) ───
                if (!skipXts)
                foreach (var (method, dataKey, tweakKey) in allKeys)
                {
                    if (found) break;

                    // Try with and without bswap16
                    foreach (bool useBswap in new[] { true, false })
                    {
                        if (found) break;

                        string label = $"{method} [bswap16={useBswap}]";
                        var candidate = new DecryptedDiskSource(
                            new NonDisposingDiskSource(_diskSource), dataKey, tweakKey, useBswap);

                    // First, check if sector 0 decrypts to a valid PS3 partition table
                    try
                    {
                        byte[] sector0 = candidate.ReadSectors(0, 1);
                        uint magic1 = (uint)((sector0[0x14] << 24) | (sector0[0x15] << 16) |
                                              (sector0[0x16] << 8) | sector0[0x17]);
                        uint magic2 = (uint)((sector0[0x1C] << 24) | (sector0[0x1D] << 16) |
                                              (sector0[0x1E] << 8) | sector0[0x1F]);

                        bool validPartTable = (magic1 == 0x0FACE0FF || magic2 == 0xDEADFACE);

                        if (validPartTable)
                        {
                            Log($"  [{label}] Sector 0 valid! (magic: {magic1:X8} / {magic2:X8})");
                        }
                        else
                        {
                            Log($"  [{label}] Sector 0 magic: {magic1:X8} / {magic2:X8} (expected 0FACE0FF/DEADFACE)");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log($"  [{label}] Sector 0 read error: {ex.Message}");
                    }

                    // Scan candidate partition offsets for UFS2 superblock
                    foreach (long partStart in CandidatePartitionStarts)
                    {
                        if (found) break;

                        long sbByteOffset = (partStart * 512) + 65536;
                        if (sbByteOffset + 8192 > _diskSource.TotalSize) continue;

                        try
                        {
                            byte[] sbData = candidate.ReadBytes(sbByteOffset, 8192);

                            if (sbData.Length > 0x560)
                            {
                                uint magic = (uint)((sbData[0x55C] << 24) | (sbData[0x55D] << 16) |
                                                     (sbData[0x55E] << 8) | sbData[0x55F]);

                                if (magic == 0x19540119)
                                {
                                    found = true;
                                    foundMethod = label;
                                    foundPartitionSector = partStart;
                                    foundBswap = useBswap;

                                    _decryptedSource?.Dispose();
                                    _decryptedSource = new DecryptedDiskSource(
                                        _diskSource, dataKey, tweakKey, useBswap);
                                    break;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Log($"    [{label}] partition 0x{partStart:X}: error: {ex.Message}");
                        }
                    }

                    if (!found)
                    {
                        candidate.Dispose();
                    }
                    } // end bswap foreach
                }

                } // end if (!found) — full scan block
            });

            if (found)
            {
                Log($"SUCCESS: UFS2 superblock found!");
                Log($"  Key method: {foundMethod}");
                Log($"  Partition start: sector 0x{foundPartitionSector:X} ({foundPartitionSector})");

                // Create the appropriate decrypted source
                if (foundCbc)
                {
                    _decryptedSource?.Dispose();
                    var cbcSource = new DecryptedDiskSourceCbc(
                        _diskSource, cbcKeys.AtaDataKey, foundBswap);
                    _fileSystem = new Ufs2FileSystem(cbcSource, foundPartitionSector);

                    // Store for reopening with write access
                    _cbcKey = (byte[])cbcKeys.AtaDataKey.Clone();
                    _cbcBswap = foundBswap;
                    _partitionSector = foundPartitionSector;
                    _isXts = false;
                }
                else
                {
                    // XTS path — _decryptedSource already set during scan
                    _fileSystem = new Ufs2FileSystem(_decryptedSource!, foundPartitionSector);
                    _partitionSector = foundPartitionSector;
                    _xtsBswap = foundBswap;
                    _isXts = true;

                    // Find and store the XTS keys from the successful method
                    foreach (var (method, dk, tk) in allKeys)
                    {
                        if (foundMethod.Contains(method))
                        {
                            _xtsDataKey = (byte[])dk.Clone();
                            _xtsTweakKey = (byte[])tk.Clone();
                            break;
                        }
                    }
                }

                IsDecrypted = true;
                EncryptionHint = ""; // Clear hint after use

                // Build the disk layout with the discovered partition
                _diskLayout = new Ps3DiskLayout
                {
                    Partitions = new List<Ps3Partition>()
                };

                if (foundPartitionSector > 0)
                {
                    _diskLayout.Partitions.Add(new Ps3Partition
                    {
                        Index = 0,
                        Name = "System",
                        StartSector = 0,
                        SectorCount = foundPartitionSector,
                        Type = Ps3PartitionType.System
                    });
                }

                _diskLayout.Partitions.Add(new Ps3Partition
                {
                    Index = _diskLayout.Partitions.Count,
                    Name = "GameOS (UFS2)",
                    StartSector = foundPartitionSector,
                    SectorCount = _diskSource.SectorCount - foundPartitionSector,
                    Type = Ps3PartitionType.GameOS
                });

                _diskLayout.DataRegionStartSector = foundPartitionSector;
                _diskLayout.DataRegionSectorCount = _diskSource.SectorCount - foundPartitionSector;

                Partitions.Clear();
                foreach (var p in _diskLayout.Partitions)
                    Partitions.Add(p);

                if (DiskInfo != null)
                {
                    DiskInfo.IsDecrypted = true;
                    DiskInfo.PartitionCount = Partitions.Count;
                    DiskInfo.Status = "Decrypted — mounting UFS2 filesystem...";
                }

                StatusText = "Decryption successful. Mounting filesystem...";
                await MountFilesystemAsync();
            }
            else
            {
                LogSeparator();
                Log("FAILED: No valid UFS2 superblock found with any key/offset/bswap combination.");
                Log("");

                // Dump CBC-192 decrypted sector 0 (most likely for CECHA)
                foreach (bool bswap in new[] { false, true })
                {
                    try
                    {
                        string lbl = $"AES-CBC-192 [bswap16={bswap}]";
                        using var cbcDebug = new DecryptedDiskSourceCbc(
                            new NonDisposingDiskSource(_diskSource), cbcKeys.AtaDataKey, bswap);
                        byte[] s0 = cbcDebug.ReadSectors(0, 1);
                        Log($"[{lbl}] Decrypted sector 0 (first 64 bytes):");
                        Log($"  {BitConverter.ToString(s0, 0, 64)}");
                        Log("");
                    }
                    catch (Exception ex)
                    {
                        Log($"[CBC-192 bswap={bswap}] Error: {ex.Message}");
                    }
                }

                // Also dump first XTS key for comparison
                var (m, dk, tk) = allKeys[0];
                foreach (bool bswap in new[] { true, false })
                {
                    try
                    {
                        string lbl = $"{m} [bswap16={bswap}]";
                        using var debugSource = new DecryptedDiskSource(
                            new NonDisposingDiskSource(_diskSource), dk, tk, bswap);
                        byte[] s0 = debugSource.ReadSectors(0, 1);
                        Log($"[{lbl}] Decrypted sector 0 (first 64 bytes):");
                        Log($"  {BitConverter.ToString(s0, 0, 64)}");
                        Log("");
                    }
                    catch (Exception ex)
                    {
                        Log($"[{m} bswap={bswap}] Error: {ex.Message}");
                    }
                }

                Log($"Log file saved to: {LogFilePath}");
                LogSeparator();

                IsDecrypted = true;
                StatusText = $"No UFS2 found. Log saved to Desktop (PS3HddTool_log.txt).";

                if (DiskInfo != null)
                {
                    DiskInfo.IsDecrypted = true;
                    DiskInfo.Status = "Decrypted but UFS2 not found — see log on Desktop.";
                }
            }
        }
        catch (Exception ex)
        {
            StatusText = $"Decryption error: {ex.Message}";
            Log($"ERROR during decryption: {ex.Message}");
        }
        finally
        {
            IsBusy = false;
            IsProgressIndeterminate = false;
        }
    }

    /// <summary>
    /// Mount the UFS2 filesystem from the GameOS partition.
    /// </summary>
    private async Task MountFilesystemAsync()
    {
        // If _fileSystem was already created (CBC path), just mount it
        if (_fileSystem != null && _diskLayout != null)
        {
            try
            {
                bool mounted = false;
                await Task.Run(() => { mounted = _fileSystem.Mount(); });

                if (!mounted)
                {
                    StatusText = "UFS2 mount failed during detailed parse.";
                    Log("ERROR: UFS2 mount failed during detailed superblock parse.");
                    return;
                }

                IsFilesystemMounted = true;
                var sb2 = _fileSystem.Superblock!;
                Log($"UFS2 mounted: {sb2.CylinderGroups} CGs, block={sb2.BlockSize}, frag={sb2.FragmentSize}, ipg={sb2.InodesPerGroup}");

                if (DiskInfo != null)
                {
                    DiskInfo.HasValidUfs2 = true;
                    DiskInfo.VolumeName = sb2.VolumeName;
                    DiskInfo.Status = "Filesystem mounted and ready.";
                }

                StatusText = "Filesystem mounted. Loading root directory...";
                await LoadDirectoryTreeAsync();
                return;
            }
            catch (Exception ex)
            {
                StatusText = $"Mount error: {ex.Message}";
                Log($"ERROR mounting filesystem: {ex.Message}");
                return;
            }
        }

        if (_decryptedSource == null || _diskLayout == null) return;

        try
        {
            var gameOsPartition = _diskLayout.Partitions
                .FirstOrDefault(p => p.Type == Ps3PartitionType.GameOS);

            if (gameOsPartition == null)
            {
                StatusText = "No GameOS partition found.";
                Log("WARNING: Could not locate GameOS partition.");
                return;
            }

            Log($"Mounting UFS2 from partition at sector 0x{gameOsPartition.StartSector:X}...");

            bool mounted = false;
            await Task.Run(() =>
            {
                _fileSystem = new Ufs2FileSystem(_decryptedSource, gameOsPartition.StartSector);
                mounted = _fileSystem.Mount();
            });

            if (!mounted)
            {
                // This shouldn't happen since we already validated the superblock, but just in case
                StatusText = "UFS2 mount failed during detailed parse.";
                Log("ERROR: UFS2 mount failed during detailed superblock parse.");
                return;
            }

            IsFilesystemMounted = true;
            var sb = _fileSystem!.Superblock!;
            Log($"UFS2 mounted: {sb.CylinderGroups} CGs, block={sb.BlockSize}, frag={sb.FragmentSize}, ipg={sb.InodesPerGroup}");

            if (DiskInfo != null)
            {
                DiskInfo.HasValidUfs2 = true;
                DiskInfo.VolumeName = sb.VolumeName;
                DiskInfo.Status = "Filesystem mounted and ready.";
            }

            StatusText = "Filesystem mounted. Loading root directory...";
            await LoadDirectoryTreeAsync();
        }
        catch (Exception ex)
        {
            StatusText = $"Mount error: {ex.Message}";
            Log($"ERROR mounting filesystem: {ex.Message}");
        }
    }

    /// <summary>
    /// Load the root directory into the tree view.
    /// </summary>
    private async Task LoadDirectoryTreeAsync()
    {
        if (_fileSystem == null) return;

        try
        {
            List<FileTreeNode> rootChildren = new();

            await Task.Run(() =>
            {
                var rootInode = _fileSystem.ReadInode(2);
                var entries = _fileSystem.ReadDirectory(rootInode);

                // Dump root inode
                Log($"=== ROOT INODE DUMP ===");
                if (rootInode.RawBytes != null)
                {
                    for (int r = 0; r < Math.Min(256, rootInode.RawBytes.Length); r += 32)
                        Log($"  0x{r:X2}: {BitConverter.ToString(rootInode.RawBytes, r, Math.Min(32, rootInode.RawBytes.Length - r))}");
                }
                Log($"  Mode=0x{rootInode.Mode:X4} nlink={rootInode.LinkCount} size={rootInode.Size} blocks={rootInode.Blocks}");

                // Dump raw root directory data
                var rootDirData = _fileSystem.ReadInodeData(rootInode);
                Log($"=== ROOT DIR DATA ({rootDirData.Length} bytes) ===");
                for (int r = 0; r < Math.Min(512, rootDirData.Length); r += 32)
                    Log($"  0x{r:X2}: {BitConverter.ToString(rootDirData, r, Math.Min(32, rootDirData.Length - r))}");

                foreach (var entry in entries)
                {
                    if (entry.Name == "." || entry.Name == "..") continue;

                    try
                    {
                        var inode = _fileSystem.ReadInode(entry.InodeNumber);
                        var node = FileTreeNode.FromInode(inode, entry.Name, "/");
                        rootChildren.Add(node);

                        // Log inode details for every root child
                        uint rootChildBlksize = inode.RawBytes != null && inode.RawBytes.Length >= 16
                            ? System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(inode.RawBytes.AsSpan(0x0C)) : 0;
                        Log($"  [{entry.Name}] inode={entry.InodeNumber} mode=0x{inode.Mode:X4} nlink={inode.LinkCount} size={inode.Size} uid={inode.Uid} gid={inode.Gid} flags=0x{inode.Flags:X8} blksize={rootChildBlksize}");
                        if (inode.RawBytes != null && inode.RawBytes.Length >= 32)
                            Log($"    raw: {BitConverter.ToString(inode.RawBytes, 0, 32)}");

                        // Deep dump for packages directory
                        if (entry.Name == "packages" && inode.FileType == Ufs2FileType.Directory)
                        {
                            Log($"=== PACKAGES INODE {entry.InodeNumber} DUMP ===");
                            if (inode.RawBytes != null)
                            {
                                for (int r = 0; r < Math.Min(256, inode.RawBytes.Length); r += 32)
                                    Log($"  0x{r:X2}: {BitConverter.ToString(inode.RawBytes, r, Math.Min(32, inode.RawBytes.Length - r))}");
                            }
                            Log($"  Mode=0x{inode.Mode:X4} nlink={inode.LinkCount} uid={inode.Uid} gid={inode.Gid} size={inode.Size} blocks={inode.Blocks}");

                            // Dump packages directory data block
                            var pkgDirData = _fileSystem.ReadInodeData(inode);
                            Log($"=== PACKAGES DIR DATA ({pkgDirData.Length} bytes) ===");
                            for (int r = 0; r < Math.Min(512, pkgDirData.Length); r += 32)
                                Log($"  0x{r:X2}: {BitConverter.ToString(pkgDirData, r, Math.Min(32, pkgDirData.Length - r))}");

                            // Dump inode of first .pkg file in packages directory
                            try
                            {
                                var pkgEntries = _fileSystem.ReadDirectory(inode);
                                foreach (var pe in pkgEntries)
                                {
                                    if (pe.Name.EndsWith(".pkg", StringComparison.OrdinalIgnoreCase))
                                    {
                                        var pkgInode = _fileSystem.ReadInode(pe.InodeNumber);
                                        Log($"=== PKG FILE INODE {pe.InodeNumber} ({pe.Name}) ===");
                                        if (pkgInode.RawBytes != null)
                                        {
                                            for (int r = 0; r < pkgInode.RawBytes.Length; r += 32)
                                                Log($"  0x{r:X2}: {BitConverter.ToString(pkgInode.RawBytes, r, Math.Min(32, pkgInode.RawBytes.Length - r))}");
                                        }
                                        Log($"  size={pkgInode.Size} blocks={pkgInode.Blocks} mode=0x{pkgInode.Mode:X4}");
                                        break; // just dump the first one
                                    }
                                }
                            }
                            catch { }
                            // Dump the CG that contains this inode
                            long ipg = _fileSystem.Superblock!.InodesPerGroup;
                            int cgNum = (int)(entry.InodeNumber / ipg);
                            long cgOffset = _fileSystem.PartitionOffsetBytes + (long)cgNum * _fileSystem.Superblock.FragsPerGroup * _fileSystem.Superblock.FragmentSize;
                            int fsCblkno = BinaryPrimitives.ReadInt32BigEndian(_fileSystem.RawSuperblockData.AsSpan(0x0C));
                            long cgHeaderOffset = cgOffset + fsCblkno * _fileSystem.Superblock.FragmentSize;
                            byte[] cgHeader = _fileSystem.DiskSource.ReadBytes(cgHeaderOffset, 256);
                            Log($"=== CG {cgNum} HEADER (inode's CG) ===");
                            for (int r = 0; r < 128; r += 32)
                                Log($"  0x{r:X2}: {BitConverter.ToString(cgHeader, r, 32)}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log($"  Skipping entry '{entry.Name}': {ex.Message}");
                    }
                }
            });

            FileTree.Clear();
            foreach (var node in rootChildren.OrderByDescending(n => n.IsDirectory).ThenBy(n => n.Name))
                FileTree.Add(node);

            StatusText = $"Ready — {FileTree.Count} items in root directory.";
            Log($"Root directory loaded: {FileTree.Count} entries.");

        }
        catch (Exception ex)
        {
            StatusText = $"Error loading directory: {ex.Message}";
            Log($"ERROR loading directory: {ex.Message}");
        }
    }

    /// <summary>
    /// Expand a directory node, loading its children on demand.
    /// </summary>
    private void DeepDumpDirectory(Ufs2FileSystem fs, long dirInodeNumber, string path, int depth)
    {
        string indent = new string(' ', depth * 2);
        try
        {
            var dirInode = fs.ReadInode(dirInodeNumber);
            var sb = fs.Superblock!;
            int fragsPerBlock = (int)(sb.BlockSize / sb.FragmentSize);
            
            // Dump the directory inode itself
            uint blksize = dirInode.RawBytes != null && dirInode.RawBytes.Length >= 16
                ? System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(dirInode.RawBytes.AsSpan(0x0C)) : 0;
            Log($"{indent}[DIR] {path}/ inode={dirInodeNumber} mode=0x{dirInode.Mode:X4} nlink={dirInode.LinkCount} size={dirInode.Size} blksize={blksize} blocks={dirInode.Blocks}");
            
            // Full 256-byte inode dump for directories
            if (dirInode.RawBytes != null)
            {
                for (int r = 0; r < Math.Min(256, dirInode.RawBytes.Length); r += 32)
                    Log($"{indent}  inode 0x{r:X2}: {BitConverter.ToString(dirInode.RawBytes, r, Math.Min(32, dirInode.RawBytes.Length - r))}");
            }
            
            // Log block pointers with alignment info
            for (int i = 0; i < 12; i++)
            {
                long dbAddr = dirInode.DirectBlocks[i];
                if (dbAddr == 0) break;
                bool blockAligned = (dbAddr % fragsPerBlock) == 0;
                Log($"{indent}  di_db[{i}]=0x{dbAddr:X} (blockAligned={blockAligned}, frag%{fragsPerBlock}={dbAddr % fragsPerBlock})");
            }
            if (dirInode.IndirectBlock != 0)
                Log($"{indent}  di_ib[0]=0x{dirInode.IndirectBlock:X} (single indirect)");
            
            // Dump raw directory block data
            var dirData = fs.ReadInodeData(dirInode);
            Log($"{indent}  dir data ({dirData.Length} bytes, first 512): {BitConverter.ToString(dirData, 0, Math.Min(512, dirData.Length))}");
            
            // For large directories (multi-block), validate ALL entries across ALL blocks
            if (dirData.Length > 4096)
            {
                Log($"{indent}  === FULL DIR SCAN ({dirData.Length} bytes, {dirData.Length / 512} DIRBLKSIZ sections) ===");
                int scanOffset = 0;
                int entryCount = 0;
                int zeroInoCount = 0;
                int sectionIdx = 0;
                int[] entriesPerSection = new int[dirData.Length / 512 + 1];
                while (scanOffset < dirData.Length)
                {
                    int sectionEnd = Math.Min(scanOffset + 512, dirData.Length);
                    int sectionEntries = 0;
                    int posInSection = scanOffset;
                    
                    // Dump raw hex for key sections: 0, 7, 8, 9 (block boundaries)
                    if (sectionIdx == 0 || sectionIdx == 7 || sectionIdx == 8 || sectionIdx == 9 || sectionIdx == 39 || sectionIdx == 40 || sectionIdx == 130)
                    {
                        int hexLen = Math.Min(512, dirData.Length - scanOffset);
                        Log($"{indent}    RAW sec{sectionIdx} @{scanOffset} ({hexLen}b): {BitConverter.ToString(dirData, scanOffset, Math.Min(128, hexLen))}...");
                    }
                    
                    while (posInSection < sectionEnd)
                    {
                        if (posInSection + 8 > dirData.Length) break;
                        uint ino = BinaryPrimitives.ReadUInt32BigEndian(dirData.AsSpan(posInSection));
                        ushort recLen = BinaryPrimitives.ReadUInt16BigEndian(dirData.AsSpan(posInSection + 4));
                        byte dType = dirData[posInSection + 6];
                        byte nameLen = dirData[posInSection + 7];
                        
                        if (recLen == 0) { Log($"{indent}    SECTION {sectionIdx} @{posInSection}: reclen=0 BREAK"); break; }
                        
                        string eName = "";
                        if (nameLen > 0 && posInSection + 8 + nameLen <= dirData.Length)
                            eName = System.Text.Encoding.ASCII.GetString(dirData, posInSection + 8, nameLen);
                        
                        if (ino == 0) zeroInoCount++;
                        
                        // Check: does this entry cross a DIRBLKSIZ boundary?
                        int entryEnd = posInSection + recLen;
                        if (entryEnd > sectionEnd && entryEnd != sectionEnd)
                        {
                            Log($"{indent}    ERROR: entry '{eName}' @{posInSection} reclen={recLen} crosses DIRBLKSIZ boundary (section ends at {sectionEnd})!");
                        }
                        
                        // Log first, last in each section + every 200th + ino=0 entries + anomalies
                        bool isAnomaly = (recLen < 8) || (recLen > 512) || (nameLen > 255);
                        bool isZeroIno = (ino == 0);
                        if (sectionEntries == 0 || entryEnd >= sectionEnd || isAnomaly || isZeroIno || entryCount % 200 == 0)
                        {
                            string marker = isZeroIno ? " [INO=0]" : "";
                            Log($"{indent}    sec{sectionIdx} @{posInSection}: ino=0x{ino:X} reclen={recLen} type={dType} nlen={nameLen} '{eName}'{marker}");
                        }
                        
                        if (isAnomaly)
                        {
                            Log($"{indent}    ^^^ ANOMALY at offset {posInSection} in section {sectionIdx}!");
                            int dumpStart = Math.Max(0, posInSection - 16);
                            int dumpLen = Math.Min(64, dirData.Length - dumpStart);
                            Log($"{indent}    hex @{dumpStart}: {BitConverter.ToString(dirData, dumpStart, dumpLen)}");
                        }
                        
                        sectionEntries++;
                        entryCount++;
                        posInSection += recLen;
                    }
                    
                    entriesPerSection[sectionIdx] = sectionEntries;
                    sectionIdx++;
                    scanOffset = sectionEnd;
                }
                // Log per-section entry counts
                var secCounts = new System.Text.StringBuilder();
                for (int s = 0; s < sectionIdx; s++)
                    secCounts.Append($"{entriesPerSection[s]},");
                Log($"{indent}  entries/section: [{secCounts}]");
                
                // For debugging: log ALL entry names to detect duplicates
                {
                    var allNames = new System.Collections.Generic.List<string>();
                    int dScanOff = 0;
                    while (dScanOff < dirData.Length)
                    {
                        int dSecEnd = Math.Min(dScanOff + 512, dirData.Length);
                        int dPos = dScanOff;
                        while (dPos < dSecEnd)
                        {
                            if (dPos + 8 > dirData.Length) break;
                            uint dino = BinaryPrimitives.ReadUInt32BigEndian(dirData.AsSpan(dPos));
                            ushort drec = BinaryPrimitives.ReadUInt16BigEndian(dirData.AsSpan(dPos + 4));
                            if (drec == 0) break;
                            byte dnlen = dirData[dPos + 7];
                            if (dino != 0 && dnlen > 0 && dPos + 8 + dnlen <= dirData.Length)
                            {
                                string dname = System.Text.Encoding.ASCII.GetString(dirData, dPos + 8, dnlen);
                                allNames.Add(dname);
                            }
                            dPos += drec;
                        }
                        dScanOff = dSecEnd;
                    }
                    // Find duplicates
                    var nameCounts = new System.Collections.Generic.Dictionary<string, int>();
                    foreach (var n in allNames)
                    {
                        if (!nameCounts.ContainsKey(n)) nameCounts[n] = 0;
                        nameCounts[n]++;
                    }
                    var dupes = new System.Collections.Generic.List<string>();
                    foreach (var kv in nameCounts)
                        if (kv.Value > 1) dupes.Add($"'{kv.Key}'x{kv.Value}");
                    Log($"{indent}  unique names: {nameCounts.Count}, duplicates: {dupes.Count}");
                    if (dupes.Count > 0)
                        Log($"{indent}  DUPLICATES: {string.Join(", ", dupes.Take(20))}");
                }
                
                Log($"{indent}  === END FULL DIR SCAN: {entryCount} entries ({zeroInoCount} ino=0) in {sectionIdx} sections ===");
            }
            var entries = fs.ReadDirectory(dirInode);
            foreach (var entry in entries)
            {
                if (entry.Name == "." || entry.Name == "..") continue;
                
                var childInode = fs.ReadInode(entry.InodeNumber);
                uint childBlksize = childInode.RawBytes != null && childInode.RawBytes.Length >= 16
                    ? System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(childInode.RawBytes.AsSpan(0x0C)) : 0;
                
                string childPath = $"{path}/{entry.Name}";
                
                if (childInode.FileType == Ufs2FileType.Directory)
                {
                    // Recurse into subdirectories
                    DeepDumpDirectory(fs, entry.InodeNumber, childPath, depth + 1);
                }
                else
                {
                    Log($"{indent}  [FILE] {childPath} inode={entry.InodeNumber} mode=0x{childInode.Mode:X4} size={childInode.Size} blksize={childBlksize} blocks={childInode.Blocks}");
                    
                    // For files with indirect blocks or > 100KB, dump full inode + indirect pointers
                    if (childInode.IndirectBlock != 0 || childInode.Size > 100000)
                    {
                        if (childInode.RawBytes != null)
                        {
                            for (int r = 0; r < Math.Min(256, childInode.RawBytes.Length); r += 32)
                                Log($"{indent}    inode 0x{r:X2}: {BitConverter.ToString(childInode.RawBytes, r, Math.Min(32, childInode.RawBytes.Length - r))}");
                        }
                        
                        // Log direct block pointers with alignment
                        for (int i = 0; i < 12; i++)
                        {
                            long dbAddr = childInode.DirectBlocks[i];
                            if (dbAddr == 0) break;
                            bool blockAligned = (dbAddr % fragsPerBlock) == 0;
                            Log($"{indent}    di_db[{i}]=0x{dbAddr:X} (blockAligned={blockAligned}, frag%{fragsPerBlock}={dbAddr % fragsPerBlock})");
                        }
                        
                        // Dump indirect block pointer contents
                        if (childInode.IndirectBlock != 0)
                        {
                            Log($"{indent}    di_ib[0]=0x{childInode.IndirectBlock:X} (single indirect)");
                            try
                            {
                                long ibOffset = fs.PartitionOffsetBytes + childInode.IndirectBlock * sb.FragmentSize;
                                byte[] ibData = fs.DiskSource.ReadBytes(ibOffset, (int)sb.BlockSize);
                                int ptrsPerBlock = (int)(sb.BlockSize / 8);
                                int nonZero = 0;
                                for (int p = 0; p < ptrsPerBlock; p++)
                                {
                                    long ptr = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(ibData.AsSpan(p * 8));
                                    if (ptr != 0) nonZero = p + 1;
                                }
                                Log($"{indent}    indirect block: {nonZero} non-zero pointers out of {ptrsPerBlock}");
                                for (int p = 0; p < Math.Min(8, nonZero); p++)
                                {
                                    long ptr = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(ibData.AsSpan(p * 8));
                                    Log($"{indent}      ib[{p}]=0x{ptr:X}");
                                }
                                if (nonZero > 8)
                                {
                                    Log($"{indent}      ... ({nonZero - 8} more pointers)");
                                    for (int p = nonZero - 2; p < nonZero; p++)
                                    {
                                        long ptr = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(ibData.AsSpan(p * 8));
                                        Log($"{indent}      ib[{p}]=0x{ptr:X}");
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Log($"{indent}    ERROR reading indirect block: {ex.Message}");
                            }
                        }
                        if (childInode.DoubleIndirectBlock != 0)
                            Log($"{indent}    di_ib[1]=0x{childInode.DoubleIndirectBlock:X} (double indirect)");
                    }
                    else
                    {
                        // Small files: just first 32 bytes of raw inode
                        if (childInode.RawBytes != null && childInode.RawBytes.Length >= 32)
                            Log($"{indent}    raw: {BitConverter.ToString(childInode.RawBytes, 0, 32)}");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Log($"{indent}  ERROR dumping {path}: {ex.Message}");
        }
    }

    /// <summary>
    /// Dump EVERYTHING about the filesystem structure for debugging.
    /// Superblock, CG summaries, root dir, game/ dir, and CG headers for relevant CGs.
    /// </summary>
    private void DumpFilesystemDiagnostics(Ufs2FileSystem fs, long gameInodeNumber)
    {
        try
        {
            var sb = fs.Superblock!;
            var disk = fs.DiskSource;
            long partOff = fs.PartitionOffsetBytes;
            
            Log("=== FILESYSTEM DIAGNOSTICS ===");
            
            // 1. Superblock raw hex at key offsets
            byte[] sbData = fs.RawSuperblockData!;
            Log($"Superblock raw ({sbData.Length} bytes):");
            // Key fields
            int fs_sblkno = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x08));
            int fs_cblkno = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x0C));
            int fs_iblkno = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x10));
            int fs_dblkno = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x18));
            int fs_ncg = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x1C));
            int fs_bsize = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x20));
            int fs_fsize = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x24));
            int fs_frag = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x28));
            int fs_bmask = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x34));
            int fs_fmask = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x38));
            int fs_bshift = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x3C));
            int fs_fshift = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x40));
            int fs_maxcontig = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x44));
            int fs_maxbpg = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x48));
            int fs_fragshift = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x4C));
            int fs_fsbtodb = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x50));
            int fs_sbsize = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x54));
            int fs_nindir = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x5C));
            int fs_inopb = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x60));
            int fs_old_nspf = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x64));
            int fs_cssize = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x9C));
            int fs_cgsize = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0xA0));
            int fs_ipg = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0xB8));
            int fs_fpg = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0xBC));
            int fs_magic = BinaryPrimitives.ReadInt32BigEndian(sbData.AsSpan(0x55C));
            
            Log($"  fs_sblkno=0x{fs_sblkno:X} fs_cblkno=0x{fs_cblkno:X} fs_iblkno=0x{fs_iblkno:X} fs_dblkno=0x{fs_dblkno:X}");
            Log($"  fs_ncg={fs_ncg} fs_bsize={fs_bsize} fs_fsize={fs_fsize} fs_frag={fs_frag}");
            Log($"  fs_bmask=0x{fs_bmask:X} fs_fmask=0x{fs_fmask:X} fs_bshift={fs_bshift} fs_fshift={fs_fshift}");
            Log($"  fs_maxcontig={fs_maxcontig} fs_maxbpg={fs_maxbpg} fs_fragshift={fs_fragshift}");
            Log($"  fs_fsbtodb={fs_fsbtodb} fs_sbsize={fs_sbsize} fs_nindir={fs_nindir} fs_inopb={fs_inopb}");
            Log($"  fs_cssize={fs_cssize} fs_cgsize={fs_cgsize} fs_ipg={fs_ipg} fs_fpg={fs_fpg}");
            Log($"  fs_magic=0x{fs_magic:X8}");
            
            // fs_cstotal at 0x3F0
            long cs_ndir = BinaryPrimitives.ReadInt64BigEndian(sbData.AsSpan(0x3F0));
            long cs_nbfree = BinaryPrimitives.ReadInt64BigEndian(sbData.AsSpan(0x3F8));
            long cs_nifree = BinaryPrimitives.ReadInt64BigEndian(sbData.AsSpan(0x400));
            long cs_nffree = BinaryPrimitives.ReadInt64BigEndian(sbData.AsSpan(0x408));
            Log($"  fs_cstotal: ndir={cs_ndir} nbfree={cs_nbfree} nifree={cs_nifree} nffree={cs_nffree}");
            
            // fs_si (UFS2 specific)
            long fs_csaddr = BinaryPrimitives.ReadInt64BigEndian(sbData.AsSpan(0x448));
            Log($"  fs_csaddr=0x{fs_csaddr:X}");
            
            // Superblock raw dump: key regions
            Log($"  SB 0x00-0x1F: {BitConverter.ToString(sbData, 0x00, 32)}");
            Log($"  SB 0x20-0x3F: {BitConverter.ToString(sbData, 0x20, 32)}");
            Log($"  SB 0x40-0x5F: {BitConverter.ToString(sbData, 0x40, 32)}");
            Log($"  SB 0x60-0x7F: {BitConverter.ToString(sbData, 0x60, 32)}");
            Log($"  SB 0x80-0x9F: {BitConverter.ToString(sbData, 0x80, 32)}");
            Log($"  SB 0xA0-0xBF: {BitConverter.ToString(sbData, 0xA0, 32)}");
            Log($"  SB 0x3F0-0x40F: {BitConverter.ToString(sbData, 0x3F0, 32)}");
            Log($"  SB 0x440-0x45F: {BitConverter.ToString(sbData, 0x440, 32)}");
            Log($"  SB 0x550-0x56F: {BitConverter.ToString(sbData, 0x550, 32)}");
            
            // 2. Root inode (inode 2) full dump
            Log($"\n=== ROOT INODE (inode 2) ===");
            var rootInode = fs.ReadInode(2);
            if (rootInode.RawBytes != null)
            {
                for (int r = 0; r < Math.Min(256, rootInode.RawBytes.Length); r += 32)
                    Log($"  0x{r:X2}: {BitConverter.ToString(rootInode.RawBytes, r, Math.Min(32, rootInode.RawBytes.Length - r))}");
            }
            
            // Root directory data
            var rootData = fs.ReadInodeData(rootInode);
            Log($"=== ROOT DIR DATA ({rootData.Length} bytes) ===");
            for (int r = 0; r < Math.Min(512, rootData.Length); r += 32)
                Log($"  0x{r:X2}: {BitConverter.ToString(rootData, r, Math.Min(32, rootData.Length - r))}");
            
            // 3. game/ inode dump
            int gameCg = (int)(gameInodeNumber / sb.InodesPerGroup);
            Log($"\n=== game/ INODE ({gameInodeNumber}, CG {gameCg}) ===");
            var gameInode = fs.ReadInode(gameInodeNumber);
            if (gameInode.RawBytes != null)
            {
                for (int r = 0; r < Math.Min(256, gameInode.RawBytes.Length); r += 32)
                    Log($"  0x{r:X2}: {BitConverter.ToString(gameInode.RawBytes, r, Math.Min(32, gameInode.RawBytes.Length - r))}");
            }
            
            // game/ directory data
            var gameData = fs.ReadInodeData(gameInode);
            Log($"=== game/ DIR DATA ({gameData.Length} bytes) ===");
            for (int r = 0; r < Math.Min(512, gameData.Length); r += 32)
                Log($"  0x{r:X2}: {BitConverter.ToString(gameData, r, Math.Min(32, gameData.Length - r))}");
            
            // 4. CG headers for game CG and neighboring CGs
            int[] cgsToCheck = new int[] { gameCg - 1, gameCg, gameCg + 1 };
            foreach (int cgi in cgsToCheck)
            {
                if (cgi < 0 || cgi >= sb.CylinderGroups) continue;
                long cgOffset = partOff + (long)cgi * sb.FragsPerGroup * sb.FragmentSize;
                long cgHeaderOffset = cgOffset + (long)fs_cblkno * sb.FragmentSize;
                byte[] cgHeader = disk.ReadBytes(cgHeaderOffset, 256);
                
                int magic = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x04));
                int cg_ndblk = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x14));
                int cg_cs_ndir = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x18));
                int cg_cs_nbfree = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x1C));
                int cg_cs_nifree = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x20));
                int cg_cs_nffree = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x24));
                int cg_rotor = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x28));
                int cg_frotor = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x2C));
                int cg_irotor = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x30));
                int cg_iusedoff = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x5C));
                int cg_freeoff = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x60));
                int cg_nextfreeoff = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x64));
                int cg_initediblk = BinaryPrimitives.ReadInt32BigEndian(cgHeader.AsSpan(0x78));
                
                Log($"\n=== CG {cgi} HEADER (magic=0x{magic:X8}) ===");
                Log($"  ndblk={cg_ndblk} ndir={cg_cs_ndir} nbfree={cg_cs_nbfree} nifree={cg_cs_nifree} nffree={cg_cs_nffree}");
                Log($"  rotor={cg_rotor} frotor={cg_frotor} irotor={cg_irotor}");
                Log($"  iusedoff=0x{cg_iusedoff:X} freeoff=0x{cg_freeoff:X} nextfreeoff=0x{cg_nextfreeoff:X} initediblk={cg_initediblk}");
                Log($"  raw 0x00-0x1F: {BitConverter.ToString(cgHeader, 0x00, 32)}");
                Log($"  raw 0x20-0x3F: {BitConverter.ToString(cgHeader, 0x20, 32)}");
                Log($"  raw 0x40-0x5F: {BitConverter.ToString(cgHeader, 0x40, 32)}");
                Log($"  raw 0x60-0x7F: {BitConverter.ToString(cgHeader, 0x60, 32)}");
                
                // Inode bitmap (first 128 bytes)
                byte[] ibitmap = disk.ReadBytes(cgHeaderOffset + cg_iusedoff, Math.Min(128, (int)sb.InodesPerGroup / 8));
                int usedInodes = 0;
                for (int b = 0; b < ibitmap.Length; b++)
                    usedInodes += System.Numerics.BitOperations.PopCount((uint)ibitmap[b]);
                Log($"  Inode bitmap: {usedInodes} used (first 64 bytes): {BitConverter.ToString(ibitmap, 0, Math.Min(64, ibitmap.Length))}");
                
                // Fragment bitmap (first 128 bytes)
                byte[] fbitmap = disk.ReadBytes(cgHeaderOffset + cg_freeoff, Math.Min(128, (int)sb.FragsPerGroup / 8));
                int freeFrags = 0;
                for (int b = 0; b < fbitmap.Length; b++)
                    freeFrags += System.Numerics.BitOperations.PopCount((uint)fbitmap[b]);
                Log($"  Frag bitmap ({freeFrags} free in first {fbitmap.Length * 8} frags): {BitConverter.ToString(fbitmap, 0, Math.Min(64, fbitmap.Length))}");
            }
            
            // 5. CS Summary table entries for relevant CGs
            if (fs_csaddr > 0 && fs_cssize > 0)
            {
                byte[] csData = disk.ReadBytes(partOff + fs_csaddr * sb.FragmentSize, Math.Min(fs_cssize, 1024));
                Log($"\n=== CS SUMMARY TABLE (first {csData.Length} bytes) ===");
                for (int cgi = 0; cgi < Math.Min(sb.CylinderGroups, csData.Length / 16); cgi++)
                {
                    int off = cgi * 16;
                    if (off + 16 > csData.Length) break;
                    int ndir = BinaryPrimitives.ReadInt32BigEndian(csData.AsSpan(off));
                    int nbfree = BinaryPrimitives.ReadInt32BigEndian(csData.AsSpan(off + 4));
                    int nifree = BinaryPrimitives.ReadInt32BigEndian(csData.AsSpan(off + 8));
                    int nffree = BinaryPrimitives.ReadInt32BigEndian(csData.AsSpan(off + 12));
                    
                    // Only log CGs near the game CG or that have interesting values
                    if (cgi == gameCg || cgi == gameCg - 1 || cgi == gameCg + 1 || cgi < 3)
                        Log($"  CG {cgi}: ndir={ndir} nbfree={nbfree} nifree={nifree} nffree={nffree}");
                }
            }
            
            Log("=== END FILESYSTEM DIAGNOSTICS ===\n");
        }
        catch (Exception ex)
        {
            Log($"ERROR in filesystem diagnostics: {ex.Message}");
        }
    }

    public async Task ExpandNodeAsync(FileTreeNode node)
    {
        if (!node.IsDirectory || node.ChildrenLoaded || _fileSystem == null) return;

        try
        {
            List<FileTreeNode> children = new();

            await Task.Run(() =>
            {
                var inode = _fileSystem.ReadInode(node.InodeNumber);
                var entries = _fileSystem.ReadDirectory(inode);

                foreach (var entry in entries)
                {
                    if (entry.Name == "." || entry.Name == "..") continue;
                    var childInode = _fileSystem.ReadInode(entry.InodeNumber);
                    var childNode = FileTreeNode.FromInode(childInode, entry.Name, node.FullPath);
                    
                    // Log inode details for diagnostics
                    uint diBlksize = childInode.RawBytes != null && childInode.RawBytes.Length >= 16 
                        ? System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(childInode.RawBytes.AsSpan(0x0C)) : 0;
                    Log($"  [{entry.Name}] inode={entry.InodeNumber} mode=0x{childInode.Mode:X4} nlink={childInode.LinkCount} size={childInode.Size} uid={childInode.Uid} gid={childInode.Gid} flags=0x{childInode.Flags:X8} blksize={diBlksize}");
                    if (childInode.RawBytes != null && childInode.RawBytes.Length >= 32)
                        Log($"    raw: {BitConverter.ToString(childInode.RawBytes, 0, 32)}");
                    
                    // Deep recursive dump for game title directories (NPUB*, NPEA*, BLUS*, BLES*, etc.)
                    if (childNode.IsDirectory && node.Name == "game")
                    {
                        // First child triggers full filesystem diagnostics
                        if (entry.Name == entries.Where(e => e.Name != "." && e.Name != "..").First().Name)
                        {
                            DumpFilesystemDiagnostics(_fileSystem, node.InodeNumber);
                        }
                        Log($"=== DEEP DUMP: game/{entry.Name} ===");
                        DeepDumpDirectory(_fileSystem, entry.InodeNumber, $"game/{entry.Name}", 0);
                        Log($"=== END DEEP DUMP: game/{entry.Name} ===");
                    }

                    // Pre-load grandchildren so the NEXT expand is also instant
                    if (childNode.IsDirectory)
                    {
                        try
                        {
                            var grandEntries = _fileSystem.ReadDirectory(childInode);
                            childNode.Children.Clear(); // remove dummy
                            
                            // Look for PARAM.SFO to enrich the display name
                            foreach (var gc in grandEntries)
                            {
                                if (gc.Name == "." || gc.Name == "..") continue;
                                var gcInode = _fileSystem.ReadInode(gc.InodeNumber);
                                childNode.Children.Add(FileTreeNode.FromInode(gcInode, gc.Name, childNode.FullPath));
                                
                                // Parse PARAM.SFO for title
                                if (gc.Name.Equals("PARAM.SFO", StringComparison.OrdinalIgnoreCase) 
                                    && gcInode.FileType == Ufs2FileType.RegularFile
                                    && gcInode.Size > 0 && gcInode.Size < 65536)
                                {
                                    try
                                    {
                                        var sfoData = _fileSystem.ReadInodeData(gcInode);
                                        var sfo = PS3HddTool.Core.FileSystem.ParamSfo.Parse(sfoData);
                                        if (sfo?.Title != null)
                                        {
                                            childNode.DisplayName = $"{entry.Name} — {sfo.Title}";
                                        }
                                    }
                                    catch { }
                                }
                            }
                            childNode.ChildrenLoaded = true;
                        }
                        catch { /* leave dummy if grandchildren fail */ }
                    }
                    
                    children.Add(childNode);
                }
            });

            node.Children.Clear();
            foreach (var child in children.OrderByDescending(c => c.IsDirectory).ThenBy(c => c.Name))
                node.Children.Add(child);

            node.ChildrenLoaded = true;
        }
        catch (Exception ex)
        {
            Log($"Error expanding {node.FullPath}: {ex.Message}");
        }
    }

    /// <summary>
    /// Extract the selected file or directory to a chosen output path.
    /// </summary>
    [RelayCommand]
    public async Task ExtractAsync((FileTreeNode Node, string OutputPath) args)
    {
        if (_fileSystem == null) return;

        try
        {
            IsBusy = true;
            IsProgressIndeterminate = false;
            ProgressValue = 0;
            var node = args.Node;
            string outputPath = args.OutputPath;

            Log($"Extracting {node.FullPath} to {outputPath}...");
            StatusText = $"Extracting {node.Name}...";

            await Task.Run(() =>
            {
                var inode = _fileSystem.ReadInode(node.InodeNumber);

                if (node.IsDirectory)
                {
                    IsProgressIndeterminate = true;
                    var progress = new Progress<string>(p =>
                        ProgressText = $"Extracting: {Path.GetFileName(p)}");

                    _fileSystem.ExtractDirectory(inode, outputPath, progress);
                }
                else
                {
                    string? dir = Path.GetDirectoryName(outputPath);
                    if (!string.IsNullOrEmpty(dir))
                        Directory.CreateDirectory(dir);

                    using var fs = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
                    long totalSize = inode.Size;
                    var startTime = DateTime.UtcNow;
                    long lastUpdate = 0;

                    _fileSystem.ExtractInodeToStream(inode, fs, bytesWritten =>
                    {
                        // Throttle UI updates to every ~500KB
                        if (bytesWritten - lastUpdate < 512 * 1024 && bytesWritten < totalSize) return;
                        lastUpdate = bytesWritten;

                        double pct = totalSize > 0 ? (double)bytesWritten / totalSize * 100 : 0;
                        ProgressValue = pct;

                        var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
                        double speedMBps = elapsed > 0.1 ? (bytesWritten / (1024.0 * 1024.0)) / elapsed : 0;

                        string eta = "";
                        if (speedMBps > 0.01 && bytesWritten < totalSize)
                        {
                            double remainMB = (totalSize - bytesWritten) / (1024.0 * 1024.0);
                            int etaSec = (int)(remainMB / speedMBps);
                            eta = etaSec >= 60 ? $" — ETA {etaSec / 60}m {etaSec % 60}s" : $" — ETA {etaSec}s";
                        }

                        ProgressText = $"{pct:F1}%  {bytesWritten / (1024 * 1024)}/{totalSize / (1024 * 1024)} MB  {speedMBps:F1} MB/s{eta}";
                    });
                }
            });

            ProgressValue = 100;
            StatusText = $"Extraction complete: {node.Name}";
            Log($"Extraction complete: {outputPath}");
        }
        catch (Exception ex)
        {
            StatusText = $"Extraction error: {ex.Message}";
            Log($"ERROR extracting: {ex.Message}");
        }
        finally
        {
            IsBusy = false;
            ProgressText = "";
            ProgressValue = 0;
            IsProgressIndeterminate = false;
        }
    }

    private static readonly string LogFilePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "PS3HddTool_log.txt");

    public void Log(string message)
    {
        string timestamped = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
        LogMessages.Add(timestamped);

        try
        {
            File.AppendAllText(LogFilePath, timestamped + Environment.NewLine);
        }
        catch { /* Don't crash if log file can't be written */ }
    }

    private void LogSeparator()
    {
        Log(new string('=', 70));
    }

    private static string FormatSize(long bytes)
    {
        string[] units = { "B", "KB", "MB", "GB", "TB" };
        double size = bytes;
        int i = 0;
        while (size >= 1024 && i < units.Length - 1) { size /= 1024; i++; }
        return $"{size:F2} {units[i]}";
    }

    private static long ReadBE64(byte[] data, int offset)
    {
        return ((long)data[offset] << 56) | ((long)data[offset + 1] << 48) |
               ((long)data[offset + 2] << 40) | ((long)data[offset + 3] << 32) |
               ((long)data[offset + 4] << 24) | ((long)data[offset + 5] << 16) |
               ((long)data[offset + 6] << 8) | data[offset + 7];
    }

    [ObservableProperty] private bool _dryRunMode = true;

    [RelayCommand]
    public async Task CreateDirectoryAsync()
    {
        if (_fileSystem == null)
        {
            StatusText = "Mount a filesystem first.";
            return;
        }

        // Default to root inode if no directory is selected
        long parentInodeNumber = 2;
        string parentName = "/";
        if (SelectedNode != null && SelectedNode.IsDirectory)
        {
            parentInodeNumber = SelectedNode.InodeNumber;
            parentName = SelectedNode.FullPath;
        }

        // Prompt for directory name
        string? name = await PromptForInput("Create Directory", "Enter new directory name:");
        if (string.IsNullOrWhiteSpace(name)) return;

        try
        {
            IsBusy = true;
            IsProgressIndeterminate = true;
            StatusText = DryRunMode ? $"[FAKE WRITE TEST] Creating directory '{name}' in {parentName}..." : $"Creating directory '{name}' in {parentName}...";

            await Task.Run(() =>
            {
                IDiskSource writeDisk;
                Ufs2FileSystem writeFs;

                if (!DryRunMode)
                {
                    writeDisk = _fileSystem.DiskSource;
                    writeFs = _fileSystem;
                }
                else
                {
                    writeDisk = _fileSystem.DiskSource;
                    writeFs = _fileSystem;
                }

                var writer = new PS3HddTool.Core.FileSystem.Ufs2Writer(
                    writeFs, writeDisk, writeFs.PartitionOffsetBytes,
                    DryRunMode, msg => Log(msg));

                writer.CreateDirectory(parentInodeNumber, name);

                if (!DryRunMode)
                {
                    writer.UpdateSuperblock();
                    Log($"Directory '{name}' created successfully.");
                    if (writeDisk != _fileSystem.DiskSource)
                        writeDisk.Dispose();
                }
                else
                    Log($"[FAKE WRITE TEST] {writer.PendingWrites.Count} writes would be performed. No data written.");
            });

            StatusText = DryRunMode 
                ? $"[FAKE WRITE TEST] Would create '{name}' — check log for details" 
                : $"Directory '{name}' created!";

            // Refresh the parent node
            if (!DryRunMode)
            {
                // Remount filesystem in place to pick up changes
                await Task.Run(() =>
                {
                    _fileSystem?.Mount();
                });

                // Full tree reload
                await LoadDirectoryTreeAsync();
            }
        }
        catch (Exception ex)
        {
            StatusText = $"Error: {ex.Message}";
            Log($"ERROR creating directory: {ex.Message}");
        }
        finally { IsBusy = false; IsProgressIndeterminate = false; ProgressValue = 0; ProgressText = ""; }
    }

    [RelayCommand]
    public async Task CopyFileToPs3Async()
    {
        if (_fileSystem == null)
        {
            StatusText = "Mount a filesystem first.";
            return;
        }
        StatusText = "Use the 'Copy File to PS3' button in the toolbar.";
    }

    public async Task CopyFileToPs3WithPath(string sourceFilePath)
    {
        if (_fileSystem == null) return;

        long parentInodeNumber = 2;
        if (SelectedNode != null && SelectedNode.IsDirectory)
            parentInodeNumber = SelectedNode.InodeNumber;

        try
        {
            IsBusy = true;
            IsProgressIndeterminate = false;
            ProgressValue = 0;
            string fileName = Path.GetFileName(sourceFilePath);
            long fileSize = new FileInfo(sourceFilePath).Length;

            StatusText = DryRunMode 
                ? $"[FAKE WRITE TEST] Copying '{fileName}' ({FormatSize(fileSize)})..." 
                : $"Copying '{fileName}' ({FormatSize(fileSize)})...";

            await Task.Run(() =>
            {
                IDiskSource writeDisk = _fileSystem.DiskSource;
                Ufs2FileSystem writeFs = _fileSystem;

                var writer = new PS3HddTool.Core.FileSystem.Ufs2Writer(
                    writeFs, writeDisk, writeFs.PartitionOffsetBytes,
                    DryRunMode, msg => Log(msg));

                using var fs = new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read);

                // Wrap in a progress-reporting stream
                var copyStart = DateTime.UtcNow;
                var progressStream = new ProgressStream(fs, fileSize, (bytesRead, total) =>
                {
                    double pct = total > 0 ? (double)bytesRead / total * 100 : 0;
                    ProgressValue = pct;
                    var elapsed = (DateTime.UtcNow - copyStart).TotalSeconds;
                    double speedMBps = elapsed > 0.1 ? (bytesRead / (1024.0 * 1024.0)) / elapsed : 0;
                    string eta = "";
                    if (speedMBps > 0.01 && bytesRead < total)
                    {
                        double remainMB = (total - bytesRead) / (1024.0 * 1024.0);
                        int etaSec = (int)(remainMB / speedMBps);
                        eta = etaSec >= 60 ? $"  ETA {etaSec / 60}m{etaSec % 60}s" : $"  ETA {etaSec}s";
                    }
                    ProgressText = $"{pct:F1}%  {FormatSize(bytesRead)}/{FormatSize(total)}  {speedMBps:F1} MB/s{eta}";
                });

                writer.WriteFile(parentInodeNumber, fileName, progressStream, fileSize);

                if (!DryRunMode)
                {
                    writer.UpdateSuperblock();
                    Log($"File '{fileName}' copied successfully.");
                }
                else
                    Log($"[FAKE WRITE TEST] {writer.PendingWrites.Count} writes would be performed. No data written.");
            });

            StatusText = DryRunMode 
                ? $"[FAKE WRITE TEST] Would copy '{fileName}' — check log for details" 
                : $"File '{fileName}' copied!";

            if (!DryRunMode)
            {
                await Task.Run(() =>
                {
                    _fileSystem?.Mount();
                });

                await LoadDirectoryTreeAsync();
            }
        }
        catch (Exception ex)
        {
            StatusText = $"Error: {ex.Message}";
            Log($"ERROR copying file: {ex.Message}");
        }
        finally { IsBusy = false; IsProgressIndeterminate = false; ProgressValue = 0; ProgressText = ""; }
    }

    /// <summary>
    /// Stream wrapper that reports read progress.
    /// </summary>
    private class ProgressStream : Stream
    {
        private readonly Stream _inner;
        private readonly long _totalSize;
        private readonly Action<long, long> _onProgress;
        private long _bytesRead;
        private long _lastReport;

        public ProgressStream(Stream inner, long totalSize, Action<long, long> onProgress)
        {
            _inner = inner;
            _totalSize = totalSize;
            _onProgress = onProgress;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int read = _inner.Read(buffer, offset, count);
            _bytesRead += read;
            if (_bytesRead - _lastReport > 256 * 1024 || _bytesRead >= _totalSize)
            {
                _lastReport = _bytesRead;
                _onProgress(_bytesRead, _totalSize);
            }
            return read;
        }

        public override bool CanRead => true;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => false;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }
        public override void Flush() => _inner.Flush();
        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    // Simple input prompt — will be overridden by UI
    private Func<string, string, Task<string?>>? _promptFunc;
    public void SetPromptFunc(Func<string, string, Task<string?>> func) => _promptFunc = func;
    private Task<string?> PromptForInput(string title, string message) =>
        _promptFunc?.Invoke(title, message) ?? Task.FromResult<string?>(null);

    public async Task CopyFolderToPs3WithPath(string sourceFolderPath)
    {
        if (_fileSystem == null) return;

        long parentInodeNumber = 2;
        if (SelectedNode != null && SelectedNode.IsDirectory)
            parentInodeNumber = SelectedNode.InodeNumber;

        try
        {
            IsBusy = true;
            IsProgressIndeterminate = false;
            ProgressValue = 0;
            string folderName = Path.GetFileName(sourceFolderPath);

            // Count total files and size
            var allFiles = Directory.GetFiles(sourceFolderPath, "*", SearchOption.AllDirectories);
            long totalSize = allFiles.Sum(f => new FileInfo(f).Length);
            int totalFiles = allFiles.Length;
            var allDirs = Directory.GetDirectories(sourceFolderPath, "*", SearchOption.AllDirectories);

            StatusText = DryRunMode
                ? $"[FAKE WRITE TEST] Copying folder '{folderName}' ({totalFiles} files, {FormatSize(totalSize)})..."
                : $"Copying folder '{folderName}' ({totalFiles} files, {FormatSize(totalSize)})...";
            Log($"[WRITE] Copying folder '{sourceFolderPath}' → PS3 ({totalFiles} files, {allDirs.Length} subdirs, {FormatSize(totalSize)})");

            await Task.Run(() =>
            {
                IDiskSource writeDisk = _fileSystem.DiskSource;
                Ufs2FileSystem writeFs = _fileSystem;

                var writer = new PS3HddTool.Core.FileSystem.Ufs2Writer(
                    writeFs, writeDisk, writeFs.PartitionOffsetBytes,
                    DryRunMode, msg => Log(msg));

                // Track created directory inodes by path
                var dirInodes = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                // Create root folder
                long rootDirInode = writer.CreateDirectory(parentInodeNumber, folderName);
                dirInodes[sourceFolderPath] = rootDirInode;
                Log($"  Created root folder '{folderName}' as inode {rootDirInode}");

                // Process directories breadth-first, writing FILES before SUBDIRS
                // at each level to match PS3 native directory entry ordering.
                int filesDone = 0;
                long bytesDone = 0;
                var startTime = DateTime.UtcNow;
                var dirsToProcess = new Queue<string>();
                dirsToProcess.Enqueue(sourceFolderPath);

                while (dirsToProcess.Count > 0)
                {
                    string currentDir = dirsToProcess.Dequeue();

                    if (!dirInodes.TryGetValue(currentDir, out long currentInode))
                    {
                        Log($"  WARNING: Inode not found for {currentDir}, skipping");
                        continue;
                    }

                    // 1. Write all FILES in this directory first
                    foreach (var file in Directory.GetFiles(currentDir))
                    {
                        string fileName = Path.GetFileName(file);
                        long fileSize = new FileInfo(file).Length;

                        string relativePath = file.Substring(sourceFolderPath.Length).TrimStart(Path.DirectorySeparatorChar);
                        ProgressText = $"{filesDone + 1}/{totalFiles} — {relativePath}";

                        using var fs = new FileStream(file, FileMode.Open, FileAccess.Read);
                        writer.WriteFile(currentInode, fileName, fs, fileSize);

                        filesDone++;
                        bytesDone += fileSize;
                        double pct = totalSize > 0 ? (double)bytesDone / totalSize * 100 : 0;
                        ProgressValue = pct;

                        var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
                        double speedMBps = elapsed > 0.1 ? (bytesDone / (1024.0 * 1024.0)) / elapsed : 0;
                        ProgressText = $"{filesDone}/{totalFiles} files  {pct:F1}%  {FormatSize(bytesDone)}/{FormatSize(totalSize)}  {speedMBps:F1} MB/s";
                    }

                    // 2. Create SUBDIRECTORIES (entries come after files)
                    foreach (var subDir in Directory.GetDirectories(currentDir))
                    {
                        string dirName = Path.GetFileName(subDir);
                        long dirInode = writer.CreateDirectory(currentInode, dirName);
                        dirInodes[subDir] = dirInode;
                        dirsToProcess.Enqueue(subDir);
                    }
                }
                Log($"  Wrote {filesDone} files, created {dirInodes.Count} directories");

                if (!DryRunMode)
                {
                    writer.UpdateSuperblock();
                    
                    // Verify CG bitmap consistency
                    writer.VerifyCgBitmaps();
                    
                    // Verify directory entry DIRSIZ constraints (catches null-terminator padding bugs)
                    Log("Verifying directory entry DIRSIZ constraints...");
                    int dirsizErrors = writer.VerifyDirectoryEntries(rootDirInode, folderName + "/");
                    if (dirsizErrors == 0)
                        Log("  DIRSIZ verification: ALL OK");
                    else
                        Log($"  DIRSIZ verification: {dirsizErrors} violations found! Data may not boot on PS3.");
                    
                    Log($"Folder '{folderName}' copied: {filesDone} files, {FormatSize(bytesDone)}");
                    if (writeDisk != _fileSystem.DiskSource)
                        writeDisk.Dispose();
                }
                else
                    Log($"[FAKE WRITE TEST] {writer.PendingWrites.Count} writes would be performed. No data written.");
            });

            ProgressValue = 100;
            StatusText = DryRunMode
                ? $"[FAKE WRITE TEST] Would copy '{folderName}' — check log"
                : $"Folder '{folderName}' copied! ({totalFiles} files, {FormatSize(totalSize)})";

            if (!DryRunMode)
            {
                await Task.Run(() =>
                {
                    _fileSystem?.Mount();
                });
                await LoadDirectoryTreeAsync();
            }
        }
        catch (Exception ex)
        {
            StatusText = $"Error: {ex.Message}";
            Log($"ERROR copying folder: {ex.Message}");
        }
        finally { IsBusy = false; IsProgressIndeterminate = false; ProgressValue = 0; ProgressText = ""; }
    }

    public void Cleanup()
    {
        _fileSystem = null;
        _decryptedSource?.Dispose();
        _diskSource?.Dispose();
    }

    public async Task ExtractPkgAsync(string pkgPath, string outputDir)
    {
        try
        {
            IsBusy = true;
            IsProgressIndeterminate = false;
            ProgressValue = 0;
            StatusText = "Parsing PKG header...";
            Log($"Opening PKG: {pkgPath}");

            string extractedDir = "";

            await Task.Run(() =>
            {
                using var pkg = new PS3HddTool.Core.Pkg.Ps3PkgReader(pkgPath, msg => Log(msg));

                Log($"PKG Content ID: {pkg.ContentId}");
                Log($"PKG Title ID: {pkg.TitleId}");
                Log($"PKG Title: {pkg.Title}");
                Log($"PKG Files: {pkg.Entries.Count}");
                Log($"PKG Data Offset: 0x{pkg.DataOffset:X}");
                Log($"PKG Data Size: {pkg.DataSize:N0} bytes");

                StatusText = $"Extracting {pkg.Entries.Count} files to {outputDir}...";

                extractedDir = pkg.ExtractAll(outputDir, (name, pct) =>
                {
                    ProgressValue = pct;
                    ProgressText = $"{pct:F0}%  {name}";
                });
            });

            ProgressValue = 100;
            StatusText = $"PKG extracted to: {extractedDir}";
            Log($"PKG extraction complete: {extractedDir}");
        }
        catch (Exception ex)
        {
            StatusText = $"PKG Error: {ex.Message}";
            Log($"ERROR extracting PKG: {ex.Message}\n{ex.StackTrace}");
        }
        finally { IsBusy = false; IsProgressIndeterminate = false; ProgressValue = 0; ProgressText = ""; }
    }
}
