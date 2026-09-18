using System.Collections.ObjectModel;
using System.Security.Cryptography;
using CommunityToolkit.Mvvm.ComponentModel;
using PS3HddTool.Core.Disk;
using PS3HddTool.Core.FileSystem;
using PS3HddTool.Core.Models;

namespace PS3HddTool.Avalonia.ViewModels;

public partial class MainViewModel
{
    private Ps4Mount? _ps4Mount;
    private int _mountGeneration;
    private readonly HashSet<FileTreeNode> _loadingPs4Nodes = new();
    [ObservableProperty] private bool _isPs4;
    [ObservableProperty] private Ps4Partition? _selectedPs4Partition;
    public ObservableCollection<Ps4Partition> Ps4Partitions { get; } = new();
    public bool CanMount => !IsBusy && IsDiskOpen && (!IsPs4 || SelectedPs4Partition?.CanBrowse == true);
    public bool CanWriteFiles => !IsBusy && IsFilesystemMounted && !IsPs4;
    public bool CanExtract => !IsBusy && IsFilesystemMounted && SelectedNode != null;
    public bool CanUsePs3Tools => !IsBusy && !IsPs4;
    public bool ShowPs3Partitions => IsDecrypted && !IsPs4;
    public int PartitionCount => IsPs4 ? Ps4Partitions.Count : Partitions.Count;
    public string KeyWatermark => IsPs4 ? "PS4 EAP HDD key (64 hex), or Import keys.bin / eap_hdd_key.bin" :
        "EID root key (96 hex), or Import hdd_key.bin / ata_data_key.bin";
    public string MountButtonText => IsPs4 ? "Mount Partition" : "Decrypt and mount";

    private void NotifyCapabilities()
    {
        OnPropertyChanged(nameof(CanMount));
        OnPropertyChanged(nameof(CanWriteFiles));
        OnPropertyChanged(nameof(CanExtract));
        OnPropertyChanged(nameof(CanUsePs3Tools));
        OnPropertyChanged(nameof(ShowPs3Partitions));
        OnPropertyChanged(nameof(PartitionCount));
        OnPropertyChanged(nameof(CanEditSelection));
        OnPropertyChanged(nameof(DetectedEncryptionType));
    }

    partial void OnIsBusyChanged(bool value) => NotifyCapabilities();
    partial void OnIsDiskOpenChanged(bool value) => NotifyCapabilities();
    partial void OnIsFilesystemMountedChanged(bool value) => NotifyCapabilities();
    partial void OnIsDecryptedChanged(bool value) => NotifyCapabilities();
    partial void OnIsPs4Changed(bool value)
    {
        OnPropertyChanged(nameof(KeyWatermark));
        OnPropertyChanged(nameof(MountButtonText));
        OnPropertyChanged(nameof(WriteModeHint));
        OnPropertyChanged(nameof(SelectionHint));
        NotifyKeyStatus();
        NotifyCapabilities();
    }
    partial void OnSelectedPs4PartitionChanged(Ps4Partition? value)
    {
        if (IsPs4)
        {
            ResetMountedFilesystem();
            StatusText = value?.CanBrowse == true ? $"Import the EAP HDD key, then mount {value.Name}. Read-only." :
                value?.Support ?? "Select a PS4 partition.";
        }
        NotifyCapabilities();
    }

    private void ResetMountedFilesystem()
    {
        _mountGeneration++;
        IsFilesystemMounted = false;
        IsDecrypted = false;
        SelectedNode = null;
        var oldPreview = ImagePreview;
        ImagePreview = null;
        oldPreview?.Dispose();
        HasImagePreview = false;
        FreeSpaceText = "";
        FileTree.Clear();
        _loadingPs4Nodes.Clear();
        _fileSystem?.DiskSource.Dispose();
        _fileSystem = null;
        _ps4Mount?.Dispose();
        _ps4Mount = null;
        _decryptedSource?.Dispose();
        _decryptedSource = null;
        _diskLayout = null;
        if (DiskInfo != null)
        {
            DiskInfo.IsDecrypted = false;
            DiskInfo.HasValidUfs2 = false;
        }
    }

    private void CloseDisk()
    {
        ResetMountedFilesystem();
        _diskSource?.Dispose();
        _diskSource = null;
        IsDiskOpen = false;
        IsPs4 = false;
        SelectedPs4Partition = null;
        Ps4Partitions.Clear();
        Partitions.Clear();
        DiskInfo = null;
        _physicalDrivePath = null;
        _physicalDriveSize = 0;
        _isXts = false;
        _cbcKey = null;
        _xtsDataKey = null;
        _xtsTweakKey = null;
        EncryptionHint = "";
    }

    private void FinishOpeningDisk(Ps4DiskLayout? layout)
    {
        IsPs4 = layout != null;
        IsDiskOpen = true;
        DiskInfo = new DiskInfo
        {
            Source = _diskSource!.Description,
            TotalSize = _diskSource.TotalSize,
            TotalSizeFormatted = FormatSize(_diskSource.TotalSize),
            IsEncrypted = true,
            Status = IsPs4 ? "PS4 disk opened read-only." : "Disk opened — enter EID Root Key and decrypt."
        };
        if (layout != null)
        {
            foreach (var partition in layout.Partitions) Ps4Partitions.Add(partition);
            SelectedPs4Partition = Ps4Partitions.FirstOrDefault(p => p.Name == "user") ??
                Ps4Partitions.FirstOrDefault(p => p.CanBrowse);
            Log($"PS4 GPT detected: {Ps4Partitions.Count} partitions. Read-only browsing supports user and eap_user.");
        }
        else StatusText = "Disk opened. Enter EID Root Key to decrypt.";
        NotifyCapabilities();
    }

    private async Task MountPs4Async()
    {
        var disk = _diskSource;
        var partition = SelectedPs4Partition;
        if (disk == null || partition?.CanBrowse != true) return;
        byte[]? key = null;
        try
        {
            IsBusy = true;
            ResetMountedFilesystem();
            IsProgressIndeterminate = true;
            StatusText = $"Mounting PS4 {partition.Name} read-only...";
            key = Ps4Mount.ParseKey(EidRootKeyHex);
            _ps4Mount = await Task.Run(() => Ps4Mount.Open(disk, partition, key));
            _fileSystem = _ps4Mount.FileSystem;
            _isXts = true;
            IsDecrypted = true;
            IsFilesystemMounted = true;
            FreeSpaceText = $"Free: {FormatSize(_fileSystem.Superblock!.FreeSpaceBytes)}";
            DiskInfo = new DiskInfo
            {
                Source = disk.Description,
                TotalSize = disk.TotalSize,
                TotalSizeFormatted = FormatSize(disk.TotalSize),
                IsEncrypted = true, IsDecrypted = true, HasValidUfs2 = true,
                PartitionCount = Ps4Partitions.Count, VolumeName = partition.Name,
                Status = $"PS4 {partition.Name} — read-only"
            };
            Log($"Mounted PS4 {partition.Name}: GPT slot {partition.Slot}, IV offset {_ps4Mount.IvOffset}, " +
                $"key byte order {(_ps4Mount.ReversedKey ? "reversed per 16 bytes" : "as supplied")}. Read-only.");
            await LoadDirectoryTreeAsync();
            StatusText = $"PS4 {partition.Name} — read-only. Select a file or folder to extract.";
        }
        catch (Exception ex)
        {
            ResetMountedFilesystem();
            StatusText = $"PS4 mount failed: {ex.Message}";
            Log(StatusText);
        }
        finally
        {
            if (key != null) CryptographicOperations.ZeroMemory(key);
            IsBusy = false;
            IsProgressIndeterminate = false;
        }
    }

    public void ImportPs4Key(byte[] data)
    {
        if (data.Length != 32) throw new InvalidDataException("A PS4 EAP HDD key file must contain exactly 32 bytes.");
        EidRootKeyHex = "EAPKEY:" + Convert.ToHexString(data);
        StatusText = "PS4 EAP HDD key imported. Select user or eap_user, then Mount Partition.";
        Log("Imported 32-byte PS4 EAP HDD key.");
    }

    private static List<FileTreeNode> ReadPs4Directory(Ufs2FileSystem fs, long inodeNumber, string path)
    {
        var children = new List<FileTreeNode>();
        foreach (var entry in fs.ReadDirectory(fs.ReadInode(inodeNumber)))
        {
            if (entry.Name is "." or "..") continue;
            children.Add(FileTreeNode.FromInode(fs.ReadInode(entry.InodeNumber), entry.Name, path, inodeNumber));
        }
        return children.OrderByDescending(n => n.IsDirectory).ThenBy(n => n.Name).ToList();
    }

    private async Task LoadPs4RootAsync()
    {
        var fs = _fileSystem;
        int generation = _mountGeneration;
        if (fs == null) return;
        var children = await Task.Run(() => ReadPs4Directory(fs, 2, "/"));
        if (generation != _mountGeneration) return;
        FileTree.Clear();
        foreach (var child in children) FileTree.Add(child);
        Log($"PS4 root loaded: {children.Count} entries.");
    }

    private async Task ExpandPs4NodeAsync(FileTreeNode node)
    {
        var fs = _fileSystem;
        int generation = _mountGeneration;
        if (IsBusy || fs == null || !node.IsDirectory || node.ChildrenLoaded || !_loadingPs4Nodes.Add(node)) return;
        try
        {
            var children = await Task.Run(() => ReadPs4Directory(fs, node.InodeNumber, node.FullPath));
            if (generation != _mountGeneration) return;
            node.Children.Clear();
            foreach (var child in children) node.Children.Add(child);
            node.ChildrenLoaded = true;
        }
        catch (Exception ex)
        {
            if (generation == _mountGeneration) { StatusText = $"Unable to read {node.FullPath}: {ex.Message}"; Log(StatusText); }
        }
        finally { _loadingPs4Nodes.Remove(node); }
    }

    private bool RejectPs4Write()
    {
        if (!IsPs4) return false;
        StatusText = "PS4 support is read-only. Use Extract to save files to your computer.";
        return true;
    }
}
