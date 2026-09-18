using System.Collections.ObjectModel;

namespace PS3HddTool.Avalonia.ViewModels;

public partial class MainViewModel
{
    private readonly RecentSourcesStore _recentSourcesStore;
    public ObservableCollection<RecentSourceEntry> RecentSources { get; } = new();

    public bool CanEditSelection => CanWriteFiles && SelectedNode != null;
    public string WriteModeHint => IsPs4
        ? "PS4 disks are opened read-only. Extract files and folders to your computer."
        : "Fake-write mode is on by default. File writes are simulated until you turn it off.";
    public string SelectionHint => IsPs4
        ? "Right-click an item to extract it or copy its path"
        : "Right-click items for extract, rename, and delete";
    public bool KeyStatusGood => KeyStatus.Good;
    public string KeyStatusText => KeyStatus.Text;

    private (bool Good, string Text) KeyStatus
    {
        get
        {
            string value = EidRootKeyHex.Trim();
            if (value.Length == 0) return (false, "");
            if (value.StartsWith("PARTIAL:", StringComparison.OrdinalIgnoreCase))
                return (false, "Import the partner key file");

            int length = IsPs4 ? 64 : 96;
            string label = IsPs4 ? "EAP HDD key · 32 bytes" : "EID root key · 48 bytes";
            if (IsPs4 && value.StartsWith("EAPKEY:", StringComparison.OrdinalIgnoreCase))
                value = value[7..];
            else if (value.StartsWith("HDDKEY:", StringComparison.OrdinalIgnoreCase))
            {
                value = value[7..];
                length = 64;
                if (!IsPs4) label = "HDD key (XTS) · 32 bytes";
            }
            else if (!IsPs4 && value.StartsWith("CBCKEY:", StringComparison.OrdinalIgnoreCase))
            {
                value = value[7..];
                label = "HDD key (CBC) · 48 bytes";
            }

            string hex = new(value.Where(c => !char.IsWhiteSpace(c) && c != '-' && c != ':').ToArray());
            if (!hex.All(Uri.IsHexDigit)) return (false, "Not valid hex");
            return hex.Length == length ? (true, label + " ✓") : (false, $"{hex.Length}/{length} hex chars");
        }
    }

    partial void OnEidRootKeyHexChanged(string value) => NotifyKeyStatus();

    private void NotifyKeyStatus()
    {
        OnPropertyChanged(nameof(KeyStatusGood));
        OnPropertyChanged(nameof(KeyStatusText));
    }

    private void RecordRecentSource(string path, string kind, long size)
    {
        for (int i = RecentSources.Count - 1; i >= 0; i--)
            if (string.Equals(RecentSources[i].Path, path, StringComparison.OrdinalIgnoreCase))
                RecentSources.RemoveAt(i);
        RecentSources.Insert(0, new RecentSourceEntry
        {
            Path = path, Kind = kind, Size = size, OpenedAt = DateTime.Now
        });
        while (RecentSources.Count > 5) RecentSources.RemoveAt(RecentSources.Count - 1);
        _recentSourcesStore.Save(RecentSources);
    }

    public void RemoveRecentSource(RecentSourceEntry entry)
    {
        RecentSources.Remove(entry);
        _recentSourcesStore.Save(RecentSources);
    }

    public void Eject()
    {
        if (IsBusy) return;
        CloseDisk();
        StatusText = "Ejected. Open a disk image or physical drive.";
        Log("Disk ejected.");
    }
}
