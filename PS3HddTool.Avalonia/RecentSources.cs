using System.Text.Json;

namespace PS3HddTool.Avalonia;

/// <summary>
/// A recently opened disk source (image file or physical drive), shown on the setup screen.
/// </summary>
public class RecentSourceEntry
{
    public string Path { get; set; } = "";
    public string Kind { get; set; } = "image"; // "image" or "drive"
    public long Size { get; set; }
    public DateTime OpenedAt { get; set; }

    public string Name => Kind == "drive" ? Path : System.IO.Path.GetFileName(Path);

    public string MetaText
    {
        get
        {
            string size = Size > 0 ? FormatSize(Size) + " · " : "";
            return size + OpenedAt.ToString("yyyy-MM-dd");
        }
    }

    private static string FormatSize(long bytes)
    {
        string[] units = { "B", "KB", "MB", "GB", "TB" };
        double size = bytes;
        int i = 0;
        while (size >= 1024 && i < units.Length - 1) { size /= 1024; i++; }
        return $"{size:F1} {units[i]}";
    }
}

/// <summary>
/// JSON persistence for recent sources, stored next to the EID key database.
/// </summary>
public sealed class RecentSourcesStore
{
    private readonly string _filePath;

    public RecentSourcesStore(string? storageDirectory = null)
    {
        _filePath = System.IO.Path.Combine(storageDirectory ?? System.IO.Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "PS3HddTool"),
            "recent_sources.json");
    }

    public List<RecentSourceEntry> Load()
    {
        try
        {
            if (File.Exists(_filePath))
            {
                var entries = JsonSerializer.Deserialize<List<RecentSourceEntry>>(File.ReadAllText(_filePath));
                if (entries != null) return entries
                    .Where(e => e != null && !string.IsNullOrWhiteSpace(e.Path) && e.Size >= 0 &&
                                (e.Kind == "image" || e.Kind == "drive"))
                    .DistinctBy(e => e.Path, StringComparer.OrdinalIgnoreCase).Take(5).ToList();
            }
        }
        catch { /* corrupt or unreadable — start fresh */ }
        return new List<RecentSourceEntry>();
    }

    public void Save(IEnumerable<RecentSourceEntry> entries)
    {
        try
        {
            Directory.CreateDirectory(System.IO.Path.GetDirectoryName(_filePath)!);
            File.WriteAllText(_filePath, JsonSerializer.Serialize(entries.ToList(),
                new JsonSerializerOptions { WriteIndented = true }));
        }
        catch { /* non-fatal */ }
    }
}
