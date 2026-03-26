using System.Collections.ObjectModel;
using PS3HddTool.Core.FileSystem;

namespace PS3HddTool.Core.Models;

/// <summary>
/// Represents a node in the filesystem tree for the GUI.
/// </summary>
public class FileTreeNode
{
    public static readonly FileTreeNode DummyChild = new() { Name = "Loading..." };

    public string Name { get; set; } = "";
    public string? DisplayName { get; set; }
    public string Label => DisplayName ?? Name;
    public string FullPath { get; set; } = "";
    public long InodeNumber { get; set; }
    public long ParentInodeNumber { get; set; }
    public bool IsDirectory { get; set; }
    public long Size { get; set; }
    public DateTime Modified { get; set; }
    public string Permissions { get; set; } = "";
    public ObservableCollection<FileTreeNode> Children { get; set; } = new();
    public bool IsExpanded { get; set; }
    public bool ChildrenLoaded { get; set; }

    public string SizeFormatted
    {
        get
        {
            if (IsDirectory) return "<DIR>";
            string[] units = { "B", "KB", "MB", "GB" };
            double size = Size;
            int i = 0;
            while (size >= 1024 && i < units.Length - 1) { size /= 1024; i++; }
            return $"{size:F1} {units[i]}";
        }
    }

    public string Icon => IsDirectory ? "📁" : GetFileIcon(Name);

    private static string GetFileIcon(string name)
    {
        string ext = Path.GetExtension(name).ToLowerInvariant();
        return ext switch
        {
            ".pkg" => "📦",
            ".sfo" => "📋",
            ".self" or ".sprx" or ".elf" => "⚙️",
            ".rif" => "🔑",
            ".edat" or ".dat" => "💾",
            ".p3t" => "🎨",
            ".mp4" or ".avi" or ".mkv" => "🎬",
            ".mp3" or ".aac" or ".at3" => "🎵",
            ".png" or ".jpg" or ".jpeg" or ".bmp" => "🖼️",
            ".txt" or ".xml" or ".json" => "📝",
            ".trp" => "🏆",
            _ => "📄"
        };
    }

    /// <summary>
    /// Build a tree node from a UFS2 directory entry and its inode.
    /// </summary>
    public static FileTreeNode FromInode(Ufs2Inode inode, string name, string parentPath, long parentInodeNumber = 0)
    {
        var node = new FileTreeNode
        {
            Name = name,
            FullPath = parentPath == "/" ? $"/{name}" : $"{parentPath}/{name}",
            InodeNumber = inode.InodeNumber,
            ParentInodeNumber = parentInodeNumber,
            IsDirectory = inode.FileType == Ufs2FileType.Directory,
            Size = inode.Size,
            Modified = inode.ModifyDateTime,
            Permissions = inode.ModeString
        };
        // Add dummy child so TreeView shows expand arrow for directories
        if (node.IsDirectory)
            node.Children.Add(DummyChild);
        return node;
    }
}

/// <summary>
/// Disk information for display in the GUI.
/// </summary>
public class DiskInfo
{
    public string Source { get; set; } = "";
    public long TotalSize { get; set; }
    public string TotalSizeFormatted { get; set; } = "";
    public bool IsEncrypted { get; set; }
    public bool IsDecrypted { get; set; }
    public bool HasValidUfs2 { get; set; }
    public string VolumeName { get; set; } = "";
    public int PartitionCount { get; set; }
    public string Status { get; set; } = "";
}
