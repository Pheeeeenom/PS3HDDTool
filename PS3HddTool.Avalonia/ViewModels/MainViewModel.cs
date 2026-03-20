using System.Buffers.Binary;
using System.Collections.ObjectModel;
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

                _diskSource = new PhysicalDiskSource(drive.Path, drive.Size);
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

                // ─── Try CBC-192 first (Fat NAND models like CECHA/B/C/E) ───
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
                        catch
                        {
                            // Continue scanning
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
                }

                IsDecrypted = true;

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

                foreach (var entry in entries)
                {
                    if (entry.Name == "." || entry.Name == "..") continue;

                    try
                    {
                        var inode = _fileSystem.ReadInode(entry.InodeNumber);
                        var node = FileTreeNode.FromInode(inode, entry.Name, "/");
                        rootChildren.Add(node);
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
    [RelayCommand]
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
                                        var sfo = ParamSfo.Parse(sfoData);
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

    private void Log(string message)
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

                if (!DryRunMode && _physicalDrivePath != null && _cbcKey != null)
                {
                    var rawDisk = new PhysicalDiskSource(_physicalDrivePath, _physicalDriveSize, writable: true);
                    writeDisk = new DecryptedDiskSourceCbc(rawDisk, _cbcKey, _cbcBswap);
                    writeFs = new Ufs2FileSystem(writeDisk, _partitionSector);
                    writeFs.Mount();
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
                // Remount filesystem to pick up changes from the write handle
                await Task.Run(() =>
                {
                    _fileSystem?.DiskSource?.Dispose();
                    if (_physicalDrivePath != null && _cbcKey != null)
                    {
                        var freshDisk = new PhysicalDiskSource(_physicalDrivePath, _physicalDriveSize);
                        var freshDecrypted = new DecryptedDiskSourceCbc(freshDisk, _cbcKey, _cbcBswap);
                        _fileSystem = new Ufs2FileSystem(freshDecrypted, _partitionSector);
                        _fileSystem.Mount();
                    }
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
                IDiskSource writeDisk;
                Ufs2FileSystem writeFs;

                if (!DryRunMode && _physicalDrivePath != null && _cbcKey != null)
                {
                    var rawDisk = new PhysicalDiskSource(_physicalDrivePath, _physicalDriveSize, writable: true);
                    writeDisk = new DecryptedDiskSourceCbc(rawDisk, _cbcKey, _cbcBswap);
                    writeFs = new Ufs2FileSystem(writeDisk, _partitionSector);
                    writeFs.Mount();
                }
                else
                {
                    writeDisk = _fileSystem.DiskSource;
                    writeFs = _fileSystem;
                }

                var writer = new PS3HddTool.Core.FileSystem.Ufs2Writer(
                    writeFs, writeDisk, writeFs.PartitionOffsetBytes,
                    DryRunMode, msg => Log(msg));

                using var fs = new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read);

                // Wrap in a progress-reporting stream
                var progressStream = new ProgressStream(fs, fileSize, (bytesRead, total) =>
                {
                    double pct = total > 0 ? (double)bytesRead / total * 100 : 0;
                    ProgressValue = pct;
                    ProgressText = $"{pct:F1}%  {FormatSize(bytesRead)}/{FormatSize(total)}";
                });

                writer.WriteFile(parentInodeNumber, fileName, progressStream, fileSize);

                if (!DryRunMode)
                {
                    Log($"File '{fileName}' copied successfully.");
                    if (writeDisk != _fileSystem.DiskSource)
                        writeDisk.Dispose();
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
                    _fileSystem?.DiskSource?.Dispose();
                    if (_physicalDrivePath != null && _cbcKey != null)
                    {
                        var freshDisk = new PhysicalDiskSource(_physicalDrivePath, _physicalDriveSize);
                        var freshDecrypted = new DecryptedDiskSourceCbc(freshDisk, _cbcKey, _cbcBswap);
                        _fileSystem = new Ufs2FileSystem(freshDecrypted, _partitionSector);
                        _fileSystem.Mount();
                    }
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
                IDiskSource writeDisk;
                Ufs2FileSystem writeFs;

                if (!DryRunMode && _physicalDrivePath != null && _cbcKey != null)
                {
                    var rawDisk = new PhysicalDiskSource(_physicalDrivePath, _physicalDriveSize, writable: true);
                    writeDisk = new DecryptedDiskSourceCbc(rawDisk, _cbcKey, _cbcBswap);
                    writeFs = new Ufs2FileSystem(writeDisk, _partitionSector);
                    writeFs.Mount();
                }
                else
                {
                    writeDisk = _fileSystem.DiskSource;
                    writeFs = _fileSystem;
                }

                var writer = new PS3HddTool.Core.FileSystem.Ufs2Writer(
                    writeFs, writeDisk, writeFs.PartitionOffsetBytes,
                    DryRunMode, msg => Log(msg));

                // Track created directory inodes by path
                var dirInodes = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                // Create root folder
                long rootDirInode = writer.CreateDirectory(parentInodeNumber, folderName);
                dirInodes[sourceFolderPath] = rootDirInode;
                Log($"  Created root folder '{folderName}' as inode {rootDirInode}");

                // Create all subdirectories (sorted by depth so parents exist first)
                foreach (var dir in allDirs.OrderBy(d => d.Count(c => c == Path.DirectorySeparatorChar)))
                {
                    string dirName = Path.GetFileName(dir);
                    string parentDir = Path.GetDirectoryName(dir)!;

                    if (!dirInodes.TryGetValue(parentDir, out long parentInode))
                    {
                        Log($"  WARNING: Parent not found for {dir}, skipping");
                        continue;
                    }

                    long dirInode = writer.CreateDirectory(parentInode, dirName);
                    dirInodes[dir] = dirInode;
                }
                Log($"  Created {allDirs.Length} subdirectories");

                // Copy all files
                int filesDone = 0;
                long bytesDone = 0;
                var startTime = DateTime.UtcNow;

                foreach (var file in allFiles)
                {
                    string fileName = Path.GetFileName(file);
                    string parentDir = Path.GetDirectoryName(file)!;
                    long fileSize = new FileInfo(file).Length;

                    if (!dirInodes.TryGetValue(parentDir, out long parentInode))
                    {
                        Log($"  WARNING: Parent not found for {file}, skipping");
                        continue;
                    }

                    string relativePath = file.Substring(sourceFolderPath.Length).TrimStart(Path.DirectorySeparatorChar);
                    ProgressText = $"{filesDone + 1}/{totalFiles} — {relativePath}";

                    using var fs = new FileStream(file, FileMode.Open, FileAccess.Read);
                    writer.WriteFile(parentInode, fileName, fs, fileSize);

                    filesDone++;
                    bytesDone += fileSize;
                    double pct = totalSize > 0 ? (double)bytesDone / totalSize * 100 : 0;
                    ProgressValue = pct;

                    var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
                    double speedMBps = elapsed > 0.1 ? (bytesDone / (1024.0 * 1024.0)) / elapsed : 0;
                    ProgressText = $"{filesDone}/{totalFiles} files  {pct:F1}%  {FormatSize(bytesDone)}/{FormatSize(totalSize)}  {speedMBps:F1} MB/s";
                }

                if (!DryRunMode)
                {
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
                    _fileSystem?.DiskSource?.Dispose();
                    if (_physicalDrivePath != null && _cbcKey != null)
                    {
                        var freshDisk = new PhysicalDiskSource(_physicalDrivePath, _physicalDriveSize);
                        var freshDecrypted = new DecryptedDiskSourceCbc(freshDisk, _cbcKey, _cbcBswap);
                        _fileSystem = new Ufs2FileSystem(freshDecrypted, _partitionSector);
                        _fileSystem.Mount();
                    }
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
}
