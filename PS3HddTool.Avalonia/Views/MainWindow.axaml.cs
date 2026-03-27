using System;
using System.Linq;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using PS3HddTool.Avalonia.ViewModels;
using PS3HddTool.Core.Models;

namespace PS3HddTool.Avalonia.Views;

public partial class MainWindow : Window
{
    private readonly MainViewModel _vm;

    public MainWindow()
    {
        InitializeComponent();
        _vm = new MainViewModel();
        DataContext = _vm;

        Closing += (_, _) => _vm.Cleanup();

        // Enable drag-drop for EID key .bin files
        DragDrop.SetAllowDrop(this, true);
        AddHandler(DragDrop.DropEvent, OnDrop);
        AddHandler(DragDrop.DragOverEvent, OnDragOver);

        var tree = this.FindControl<TreeView>("FileTreeView");
        if (tree != null)
        {
            // Selection triggers lazy load
            tree.SelectionChanged += (s, e) =>
            {
                if (tree.SelectedItem is FileTreeNode node && node.IsDirectory && !node.ChildrenLoaded)
                    _ = _vm.ExpandNodeAsync(node);
            };

            // PointerPressed bubbles up from expand arrows too — check after a short delay
            // so the TreeViewItem has had time to update IsExpanded
            tree.AddHandler(global::Avalonia.Input.InputElement.PointerPressedEvent, async (s, e) =>
            {
                await Task.Delay(50);
                
                var source = e.Source as global::Avalonia.Controls.Control;
                while (source != null && source is not TreeViewItem)
                    source = source.Parent as global::Avalonia.Controls.Control;
                
                if (source is TreeViewItem tvi && tvi.DataContext is FileTreeNode node 
                    && node.IsDirectory && !node.ChildrenLoaded)
                {
                    await _vm.ExpandNodeAsync(node);
                }
            }, global::Avalonia.Interactivity.RoutingStrategies.Tunnel);
        }
    }

    private void OnDragOver(object? sender, DragEventArgs e)
    {
        e.DragEffects = DragDropEffects.Copy;
    }

    private async void OnDrop(object? sender, DragEventArgs e)
    {
        var fileNames = e.Data.GetFiles();
        if (fileNames == null) return;

        var paths = new List<string>();
        foreach (var item in fileNames)
        {
            string? path = item.TryGetLocalPath();
            if (path != null) paths.Add(path);
        }
        if (paths.Count == 0) return;

        // Check if the first file is a key file (legacy drag-drop for EID keys)
        string firstExt = System.IO.Path.GetExtension(paths[0]).ToLowerInvariant();
        if ((firstExt == ".bin" || firstExt == ".eid") && !_vm.IsFilesystemMounted)
        {
            // Key file drag-drop (existing behavior)
            try
            {
                byte[] data = System.IO.File.ReadAllBytes(paths[0]);
                string hexKey;
                if (data.Length == 48)
                {
                    hexKey = BitConverter.ToString(data).Replace("-", "");
                }
                else if (data.Length >= 96 && data.Length <= 200)
                {
                    string text = System.Text.Encoding.ASCII.GetString(data).Trim();
                    text = text.Replace("-", "").Replace(" ", "").Replace("\r", "").Replace("\n", "");
                    if (text.Length == 96 && System.Text.RegularExpressions.Regex.IsMatch(text, "^[0-9a-fA-F]+$"))
                        hexKey = text;
                    else
                    {
                        _vm.StatusText = $"File '{System.IO.Path.GetFileName(paths[0])}' is not a valid EID key.";
                        return;
                    }
                }
                else
                {
                    _vm.StatusText = $"File '{System.IO.Path.GetFileName(paths[0])}' is not a valid EID key ({data.Length} bytes).";
                    return;
                }
                _vm.EidRootKeyHex = hexKey;
                _vm.StatusText = $"EID key loaded from {System.IO.Path.GetFileName(paths[0])}";
                _vm.Log($"EID key loaded via drag-drop: {System.IO.Path.GetFileName(paths[0])}");
            }
            catch (Exception ex)
            {
                _vm.StatusText = $"Error reading key file: {ex.Message}";
            }
            return;
        }

        // If filesystem is mounted, handle as file/folder copy to PS3
        if (_vm.IsFilesystemMounted)
        {
            // Check for PKG files
            if (paths.Count == 1 && firstExt == ".pkg")
            {
                await _vm.InstallPkgToHddAsync(paths[0]);
                return;
            }

            // Determine target directory from selected node
            long targetInode = 2; // root
            string targetName = "/";
            if (_vm.SelectedNode != null && _vm.SelectedNode.IsDirectory)
            {
                targetInode = _vm.SelectedNode.InodeNumber;
                targetName = _vm.SelectedNode.FullPath;
            }

            foreach (string path in paths)
            {
                if (Directory.Exists(path))
                {
                    _vm.Log($"Drag-drop folder: {path} → {targetName}");
                    await _vm.CopyFolderToPs3WithPath(path);
                }
                else if (File.Exists(path))
                {
                    _vm.Log($"Drag-drop file: {path} → {targetName}");
                    await _vm.CopyFileToPs3WithPath(path);
                }
            }
        }
    }

    private async void OnOpenImage(object? sender, RoutedEventArgs e)
    {
        var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = "Open PS3 HDD Image",
            AllowMultiple = false,
            FileTypeFilter = new[]
            {
                new FilePickerFileType("Disk Images") { Patterns = new[] { "*.img", "*.bin", "*.iso", "*.dd", "*.raw" } },
                new FilePickerFileType("All Files") { Patterns = new[] { "*" } }
            }
        });

        if (files.Count > 0)
        {
            string? path = files[0].TryGetLocalPath();
            if (path != null)
                await _vm.OpenImageCommand.ExecuteAsync(path);
        }
    }

    private async void OnOpenPhysicalDrive(object? sender, RoutedEventArgs e)
    {
        var dialog = new Window
        {
            Title = "Select Physical Drive",
            Width = 600,
            Height = 260,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false
        };

        var panel = new StackPanel { Margin = new global::Avalonia.Thickness(16), Spacing = 8 };

        panel.Children.Add(new TextBlock
        {
            Text = "Select a physical drive:",
            FontWeight = global::Avalonia.Media.FontWeight.SemiBold,
            Margin = new global::Avalonia.Thickness(0, 0, 0, 4)
        });

        var driveCombo = new ComboBox
        {
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Stretch,
            FontFamily = new global::Avalonia.Media.FontFamily("Consolas,Courier New,monospace"),
            FontSize = 13
        };

        // Populate drives
        var drives = new List<PS3HddTool.Core.Disk.PhysicalDriveInfo>();
        try
        {
            drives = PS3HddTool.Core.Disk.DriveEnumerator.EnumerateDrives();
            foreach (var d in drives)
                driveCombo.Items.Add(d.DisplayName);
            if (drives.Count > 0)
                driveCombo.SelectedIndex = 0;
        }
        catch { }

        if (drives.Count == 0)
        {
            driveCombo.Items.Add("No drives found (run as Administrator)");
            driveCombo.SelectedIndex = 0;
        }

        var driveRow = new Grid { ColumnDefinitions = new ColumnDefinitions("*,Auto") };
        Grid.SetColumn(driveCombo, 0);
        driveRow.Children.Add(driveCombo);

        var refreshBtn = new Button { Content = "Rescan", VerticalAlignment = global::Avalonia.Layout.VerticalAlignment.Center, Margin = new global::Avalonia.Thickness(8, 0, 0, 0) };
        Grid.SetColumn(refreshBtn, 1);
        refreshBtn.Click += (_, _) =>
        {
            driveCombo.Items.Clear();
            drives.Clear();
            try
            {
                drives = PS3HddTool.Core.Disk.DriveEnumerator.EnumerateDrives();
                foreach (var d in drives)
                    driveCombo.Items.Add(d.DisplayName);
                if (drives.Count > 0)
                    driveCombo.SelectedIndex = 0;
            }
            catch { }
            if (drives.Count == 0)
            {
                driveCombo.Items.Add("No drives found (run as Administrator)");
                driveCombo.SelectedIndex = 0;
            }
        };
        driveRow.Children.Add(refreshBtn);
        panel.Children.Add(driveRow);

        // Manual path entry as fallback
        panel.Children.Add(new TextBlock
        {
            Text = "Or enter path manually:",
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#999")),
            FontSize = 12,
            Margin = new global::Avalonia.Thickness(0, 12, 0, 0)
        });

        var pathBox = new TextBox
        {
            Watermark = "e.g. \\\\.\\PhysicalDrive2",
            FontFamily = new global::Avalonia.Media.FontFamily("Consolas,Courier New,monospace")
        };
        panel.Children.Add(pathBox);

        var buttonPanel = new StackPanel
        {
            Orientation = global::Avalonia.Layout.Orientation.Horizontal,
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Right,
            Spacing = 8,
            Margin = new global::Avalonia.Thickness(0, 16, 0, 0)
        };

        var cancelBtn = new Button { Content = "Cancel" };
        cancelBtn.Click += (_, _) => dialog.Close();

        var okBtn = new Button
        {
            Content = "Open Drive",
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#5B6EF5")),
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Colors.White)
        };
        okBtn.Click += async (_, _) =>
        {
            string path;
            long size = 0;
            
            if (!string.IsNullOrWhiteSpace(pathBox.Text))
            {
                path = pathBox.Text;
            }
            else if (driveCombo.SelectedIndex >= 0 && driveCombo.SelectedIndex < drives.Count)
            {
                var selected = drives[driveCombo.SelectedIndex];
                path = selected.Path;
                size = selected.Size;
            }
            else return;
            
            dialog.Close();
            await _vm.OpenPhysicalDriveCommand.ExecuteAsync((path, size));
        };

        buttonPanel.Children.Add(cancelBtn);
        buttonPanel.Children.Add(okBtn);
        panel.Children.Add(buttonPanel);

        dialog.Content = panel;
        await dialog.ShowDialog(this);
    }

    private readonly PS3HddTool.Core.EidKeyDatabase _keyDb = new();

    private async void OnSaveKey(object? sender, RoutedEventArgs e)
    {
        string currentKey = _vm.EidRootKeyHex;
        if (string.IsNullOrWhiteSpace(currentKey))
        {
            _vm.StatusText = "Enter or import a key first.";
            return;
        }

        if (currentKey.StartsWith("PARTIAL:", StringComparison.OrdinalIgnoreCase))
        {
            _vm.StatusText = "Cannot save incomplete key. Import both data + tweak files first.";
            return;
        }

        // Validate: either a 96-char EID root key, or a prefixed pre-derived key
        string cleanKey = currentKey.Replace("-", "").Replace(" ", "").Trim();
        bool isEidRoot = cleanKey.Length == 96 && !cleanKey.Contains(":");
        bool isPrefixed = currentKey.StartsWith("HDDKEY:", StringComparison.OrdinalIgnoreCase)
                       || currentKey.StartsWith("CBCKEY:", StringComparison.OrdinalIgnoreCase);

        if (!isEidRoot && !isPrefixed)
        {
            _vm.StatusText = "Enter a valid key: 96-char EID Root Key hex, or import a key file.";
            return;
        }

        var dialog = new Window
        {
            Title = "Save EID Key",
            Width = 400,
            Height = 180,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false
        };

        var panel = new StackPanel { Margin = new global::Avalonia.Thickness(16), Spacing = 8 };
        panel.Children.Add(new TextBlock { Text = "Enter a nickname for this key:" });
        var nameBox = new TextBox { Watermark = "e.g. My Fat CECHA, Slim 3000, etc." };
        panel.Children.Add(nameBox);

        var btnRow = new StackPanel
        {
            Orientation = global::Avalonia.Layout.Orientation.Horizontal,
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Right,
            Spacing = 8, Margin = new global::Avalonia.Thickness(0, 12, 0, 0)
        };
        var cancelBtn = new Button { Content = "Cancel" };
        cancelBtn.Click += (_, _) => dialog.Close();
        var saveBtn = new Button
        {
            Content = "Save",
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#5B6EF5")),
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Colors.White)
        };
        saveBtn.Click += (_, _) =>
        {
            string nick = nameBox.Text?.Trim() ?? "";
            if (string.IsNullOrEmpty(nick)) nick = "Unnamed";
            _keyDb.Add(nick, currentKey, _vm.DetectedEncryptionType);
            _vm.StatusText = $"Key saved as '{nick}'.";
            _vm.Log($"EID key saved: {nick}");
            dialog.Close();
        };
        btnRow.Children.Add(cancelBtn);
        btnRow.Children.Add(saveBtn);
        panel.Children.Add(btnRow);

        dialog.Content = panel;
        await dialog.ShowDialog(this);
    }

    private async void OnSavedKeys(object? sender, RoutedEventArgs e)
    {
        var entries = _keyDb.Entries;
        if (entries.Count == 0)
        {
            _vm.StatusText = "No saved keys. Use 'Save Key' to store one.";
            return;
        }

        var dialog = new Window
        {
            Title = "Saved EID Keys",
            Width = 550,
            Height = 400,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false
        };

        var panel = new StackPanel { Margin = new global::Avalonia.Thickness(16), Spacing = 8 };
        panel.Children.Add(new TextBlock
        {
            Text = "Select a key to load:",
            FontWeight = global::Avalonia.Media.FontWeight.SemiBold
        });

        var listBox = new ListBox { MinHeight = 200, MaxHeight = 280 };
        foreach (var entry in entries)
        {
            string encLabel = string.IsNullOrEmpty(entry.EncryptionType) ? "" : $" [{entry.EncryptionType}]";
            string displayHex = entry.HexKey;
            string keyType = "";
            if (displayHex.StartsWith("HDDKEY:", StringComparison.OrdinalIgnoreCase))
                { displayHex = displayHex.Substring(7); keyType = " [XTS direct]"; }
            else if (displayHex.StartsWith("CBCKEY:", StringComparison.OrdinalIgnoreCase))
                { displayHex = displayHex.Substring(7); keyType = " [CBC direct]"; }
            string shortHex = displayHex.Length >= 16 ? displayHex[..16] + "..." : displayHex;
            string display = $"{entry.Nickname}{encLabel}{keyType}  —  {shortHex}  ({entry.DateAdded:yyyy-MM-dd})";
            listBox.Items.Add(display);
        }
        if (entries.Count > 0) listBox.SelectedIndex = 0;
        panel.Children.Add(listBox);

        var btnRow = new StackPanel
        {
            Orientation = global::Avalonia.Layout.Orientation.Horizontal,
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Right,
            Spacing = 8, Margin = new global::Avalonia.Thickness(0, 12, 0, 0)
        };

        var deleteBtn = new Button
        {
            Content = "Delete",
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#FF6B6B"))
        };
        deleteBtn.Click += (_, _) =>
        {
            int idx = listBox.SelectedIndex;
            if (idx >= 0 && idx < entries.Count)
            {
                _keyDb.Remove(entries[idx].HexKey);
                listBox.Items.RemoveAt(idx);
                if (listBox.Items.Count > 0) listBox.SelectedIndex = 0;
            }
        };

        var cancelBtn = new Button { Content = "Cancel" };
        cancelBtn.Click += (_, _) => dialog.Close();

        var loadBtn = new Button
        {
            Content = "Load Key",
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#5B6EF5")),
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Colors.White)
        };
        loadBtn.Click += (_, _) =>
        {
            int idx = listBox.SelectedIndex;
            if (idx >= 0 && idx < entries.Count)
            {
                _vm.EidRootKeyHex = entries[idx].HexKey;
                _vm.EncryptionHint = entries[idx].EncryptionType;
                _vm.StatusText = $"Loaded key: {entries[idx].Nickname} [{entries[idx].EncryptionType}]";
                _vm.Log($"Loaded saved EID key: {entries[idx].Nickname} (hint: {entries[idx].EncryptionType})");
                dialog.Close();
            }
        };

        btnRow.Children.Add(deleteBtn);
        btnRow.Children.Add(cancelBtn);
        btnRow.Children.Add(loadBtn);
        panel.Children.Add(btnRow);

        dialog.Content = panel;
        await dialog.ShowDialog(this);
    }

    private async void OnImportKey(object? sender, RoutedEventArgs e)
    {
        try
        {
            var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
            {
                Title = "Import Key File(s) — select one or two files",
                AllowMultiple = true,
                FileTypeFilter = new[]
                {
                    new FilePickerFileType("Key Files") { Patterns = new[] { "*.bin" } },
                    new FilePickerFileType("All Files") { Patterns = new[] { "*" } }
                }
            });

            if (files.Count == 0) return;

            // ─── Two-file import: data key + tweak key pair ───
            if (files.Count == 2)
            {
                var paths = files.Select(f => f.Path.LocalPath).ToArray();
                var names = paths.Select(p => System.IO.Path.GetFileName(p).ToLowerInvariant()).ToArray();
                var datas = paths.Select(System.IO.File.ReadAllBytes).ToArray();

                // Both must be 16 bytes (Slim XTS) or both 24 bytes (Fat CBC-192)
                if (datas[0].Length == datas[1].Length && (datas[0].Length == 16 || datas[0].Length == 24))
                {
                    // Figure out which is data and which is tweak
                    int dataIdx = 0, tweakIdx = 1;
                    if (names[1].Contains("data") && !names[0].Contains("data"))
                        { dataIdx = 1; tweakIdx = 0; }
                    else if (names[0].Contains("tweak") && !names[1].Contains("tweak"))
                        { dataIdx = 1; tweakIdx = 0; }

                    byte[] dataKey = datas[dataIdx];
                    byte[] tweakKey = datas[tweakIdx];
                    int keyLen = dataKey.Length;

                    // Combine into hdd_key
                    byte[] combined = new byte[keyLen * 2];
                    Buffer.BlockCopy(dataKey, 0, combined, 0, keyLen);
                    Buffer.BlockCopy(tweakKey, 0, combined, keyLen, keyLen);
                    string hex = BitConverter.ToString(combined).Replace("-", "");

                    _vm.Log($"Imported key pair: {names[dataIdx]} (data) + {names[tweakIdx]} (tweak)");
                    _vm.Log($"  Data key  ({keyLen}B): {BitConverter.ToString(dataKey)}");
                    _vm.Log($"  Tweak key ({keyLen}B): {BitConverter.ToString(tweakKey)}");

                    if (keyLen == 24)
                    {
                        _vm.EidRootKeyHex = "CBCKEY:" + hex;
                        _vm.StatusText = "Pre-derived HDD key imported (CBC-192, 2×24 bytes). Press Decrypt.";
                    }
                    else
                    {
                        _vm.EidRootKeyHex = "HDDKEY:" + hex;
                        _vm.StatusText = "Pre-derived HDD key imported (XTS-128, 2×16 bytes). Press Decrypt.";
                    }
                }
                else
                {
                    _vm.StatusText = $"Key pair must be same size (16B or 24B each). Got {datas[0].Length}B + {datas[1].Length}B.";
                    _vm.Log($"Import failed: mismatched key sizes {datas[0].Length} and {datas[1].Length}");
                }
                return;
            }

            // ─── Single-file import ───
            var file = files[0];
            string path = file.Path.LocalPath;
            string fileName = System.IO.Path.GetFileName(path).ToLowerInvariant();
            byte[] data = System.IO.File.ReadAllBytes(path);

            if (data.Length == 48 && IsLikelyHddKey(fileName))
            {
                // Fat CBC-192 hdd_key.bin (48 bytes: 24B data + 24B tweak)
                string hex = BitConverter.ToString(data).Replace("-", "");
                _vm.Log($"Imported Fat HDD key (CBC-192) from: {fileName} ({data.Length} bytes)");
                _vm.Log($"  Data key  (0-23):  {BitConverter.ToString(data[..24])}");
                _vm.Log($"  Tweak key (24-47): {BitConverter.ToString(data[24..48])}");
                _vm.EidRootKeyHex = "CBCKEY:" + hex;
                _vm.StatusText = "Pre-derived Fat HDD key imported (CBC-192, 48 bytes). Press Decrypt.";
            }
            else if (data.Length == 48)
            {
                // eid_root_key.bin (48 bytes: 32B key + 16B IV)
                string hex = BitConverter.ToString(data).Replace("-", "");
                _vm.EidRootKeyHex = hex;
                _vm.Log($"Imported EID Root Key from: {fileName} ({data.Length} bytes)");
                _vm.Log($"  ERK key (0-31):  {BitConverter.ToString(data[..32])}");
                _vm.Log($"  ERK IV  (32-47): {BitConverter.ToString(data[32..48])}");
                _vm.StatusText = "EID Root Key imported. Press Decrypt to derive HDD keys.";
            }
            else if (data.Length == 32)
            {
                // hdd_key.bin / vflash_key.bin for XTS (32 bytes: 16B data + 16B tweak)
                string hex = BitConverter.ToString(data).Replace("-", "");
                _vm.Log($"Imported pre-derived key from: {fileName} ({data.Length} bytes)");
                _vm.Log($"  Data key  (0-15):  {BitConverter.ToString(data[..16])}");
                _vm.Log($"  Tweak key (16-31): {BitConverter.ToString(data[16..32])}");
                _vm.EidRootKeyHex = "HDDKEY:" + hex;
                _vm.StatusText = "Pre-derived HDD key imported (XTS-128, 32 bytes). Press Decrypt.";
            }
            else if (data.Length == 16)
            {
                // Single 16-byte key component — needs its partner
                string hex = BitConverter.ToString(data).Replace("-", "");
                string component = DetectKeyComponent(fileName);
                _vm.Log($"Imported single key component from: {fileName} ({data.Length} bytes)");
                _vm.Log($"  Detected as: {component}");
                _vm.Log($"  Key: {BitConverter.ToString(data)}");
                _vm.StatusText = $"Single key imported ({component}). Import its partner too — select both files at once.";
                // Store temporarily; user needs to import the pair together
                _vm.EidRootKeyHex = $"PARTIAL:{component}:{hex}";
            }
            else if (data.Length == 24)
            {
                // Single 24-byte CBC key component — needs its partner
                string hex = BitConverter.ToString(data).Replace("-", "");
                string component = DetectKeyComponent(fileName);
                _vm.Log($"Imported single CBC key component from: {fileName} ({data.Length} bytes)");
                _vm.Log($"  Detected as: {component}");
                _vm.Log($"  Key: {BitConverter.ToString(data)}");
                _vm.StatusText = $"Single key imported ({component}, 24B). Import its partner too — select both files at once.";
                _vm.EidRootKeyHex = $"PARTIAL:{component}:{hex}";
            }
            else
            {
                _vm.StatusText = $"Unexpected key file size: {data.Length} bytes.";
                _vm.Log($"Import failed: {fileName} is {data.Length} bytes");
                _vm.Log($"  Supported sizes: 16B (single key), 24B (single CBC key), 32B (hdd_key XTS), 48B (eid_root_key or hdd_key CBC)");
            }
        }
        catch (Exception ex)
        {
            _vm.StatusText = $"Import error: {ex.Message}";
        }
    }

    /// <summary>
    /// Detect if a 48-byte file is likely a pre-derived hdd_key (24+24 Fat CBC)
    /// rather than an eid_root_key, based on filename patterns.
    /// </summary>
    private static bool IsLikelyHddKey(string fileNameLower)
    {
        return fileNameLower.Contains("hdd_key") || fileNameLower.Contains("hddkey")
            || fileNameLower.Contains("ata_key") || fileNameLower.Contains("atakey")
            || fileNameLower.Contains("flash_key") || fileNameLower.Contains("flashkey")
            || fileNameLower.Contains("vflash_key") || fileNameLower.Contains("vflashkey");
    }

    /// <summary>
    /// Try to identify a key component from its filename.
    /// </summary>
    private static string DetectKeyComponent(string fileNameLower)
    {
        if (fileNameLower.Contains("encdec") || fileNameLower.Contains("vflash") || fileNameLower.Contains("flash"))
        {
            if (fileNameLower.Contains("tweak")) return "encdec_tweak";
            return "encdec_data";
        }
        if (fileNameLower.Contains("tweak")) return "ata_tweak";
        return "ata_data";
    }

    private async void OnExportKeys(object? sender, RoutedEventArgs e)
    {
        try
        {
            string currentKey = _vm.EidRootKeyHex;
            if (string.IsNullOrWhiteSpace(currentKey))
            {
                _vm.StatusText = "Enter or import an EID Root Key first.";
                return;
            }

            // Only EID root keys can derive individual key files
            if (currentKey.StartsWith("HDDKEY:", StringComparison.OrdinalIgnoreCase) ||
                currentKey.StartsWith("CBCKEY:", StringComparison.OrdinalIgnoreCase))
            {
                _vm.StatusText = "Export requires an EID Root Key — pre-derived keys cannot be re-derived into components.";
                return;
            }

            if (currentKey.StartsWith("PARTIAL:", StringComparison.OrdinalIgnoreCase))
            {
                _vm.StatusText = "Cannot export: incomplete key.";
                return;
            }

            byte[] eidRootKey;
            try
            {
                eidRootKey = PS3HddTool.Core.Crypto.Ps3KeyDerivation.ParseEidRootKey(currentKey);
            }
            catch (Exception ex)
            {
                _vm.StatusText = $"Invalid EID Root Key: {ex.Message}";
                return;
            }

            // Let user pick model type
            var dialog = new Window
            {
                Title = "Export Derived Keys",
                Width = 460,
                Height = 330,
                WindowStartupLocation = WindowStartupLocation.CenterOwner,
                CanResize = false,
                Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#0A0A10"))
            };

            var panel = new StackPanel { Margin = new global::Avalonia.Thickness(20), Spacing = 10 };
            panel.Children.Add(new TextBlock
            {
                Text = "Select your PS3 model to derive the correct keys:",
                FontWeight = global::Avalonia.Media.FontWeight.SemiBold,
                Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Colors.White)
            });

            var modelCombo = new ComboBox { MinWidth = 380 };
            modelCombo.Items.Add("Fat NAND (CECHA/B/C/E) — AES-CBC-192, 24-byte ATA keys");
            modelCombo.Items.Add("Fat NOR (CECHG/H/J/K/L/M) — AES-XTS-128, 16-byte ATA keys");
            modelCombo.Items.Add("Slim (CECH-2xxx) — AES-XTS-128, 16-byte ATA keys");
            modelCombo.Items.Add("All variants (export every key size)");
            modelCombo.SelectedIndex = 3; // Default to all

            panel.Children.Add(modelCombo);

            panel.Children.Add(new TextBlock
            {
                Text = "Files that will be created:\n" +
                       "  • ata_data_key.bin, ata_tweak_key.bin\n" +
                       "  • encdec_data_key.bin, encdec_tweak_key.bin\n" +
                       "  • hdd_key.bin (combined ATA data+tweak)\n" +
                       "  • vflash_key.bin (combined ENCDEC data+tweak)",
                FontSize = 12,
                Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#8899BB")),
                FontFamily = new global::Avalonia.Media.FontFamily("Consolas,Courier New,monospace")
            });

            var btnRow = new StackPanel
            {
                Orientation = global::Avalonia.Layout.Orientation.Horizontal,
                HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Right,
                Spacing = 8, Margin = new global::Avalonia.Thickness(0, 12, 0, 0)
            };
            var cancelBtn = new Button { Content = "Cancel" };
            cancelBtn.Click += (_, _) => dialog.Close();
            var exportBtn = new Button
            {
                Content = "Choose Folder & Export",
                Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#5B6EF5")),
                Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Colors.White)
            };

            int selectedModel = -1;
            exportBtn.Click += (_, _) =>
            {
                selectedModel = modelCombo.SelectedIndex;
                dialog.Close();
            };

            btnRow.Children.Add(cancelBtn);
            btnRow.Children.Add(exportBtn);
            panel.Children.Add(btnRow);
            dialog.Content = panel;

            await dialog.ShowDialog(this);

            if (selectedModel < 0) return; // cancelled

            // Pick output folder
            var folders = await StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
            {
                Title = "Select Output Folder for Key Files",
                AllowMultiple = false
            });
            if (folders == null || folders.Count == 0) return;
            string outDir = folders[0].Path.LocalPath;

            // Derive keys
            var xtsKeys = PS3HddTool.Core.Crypto.Ps3KeyDerivation.DeriveKeys(eidRootKey);
            var cbcKeys = PS3HddTool.Core.Crypto.Ps3KeyDerivation.DeriveKeysFatNand(eidRootKey);

            int filesWritten = 0;

            // Helper to write a key file
            void WriteKey(string name, byte[] keyData)
            {
                string path = System.IO.Path.Combine(outDir, name);
                System.IO.File.WriteAllBytes(path, keyData);
                _vm.Log($"  Exported: {name} ({keyData.Length} bytes) — {BitConverter.ToString(keyData)}");
                filesWritten++;
            }

            _vm.Log("─── Key Export ───");
            _vm.Log($"EID Root Key: {BitConverter.ToString(eidRootKey[..8])}...");

            bool doXts = selectedModel == 1 || selectedModel == 2 || selectedModel == 3;
            bool doCbc = selectedModel == 0 || selectedModel == 3;

            if (doCbc)
            {
                _vm.Log("Fat NAND (CBC-192, 24-byte keys):");
                string prefix = selectedModel == 3 ? "fat_nand_" : "";
                WriteKey($"{prefix}ata_data_key.bin", cbcKeys.AtaDataKey);
                WriteKey($"{prefix}ata_tweak_key.bin", cbcKeys.AtaTweakKey);
                WriteKey($"{prefix}encdec_data_key.bin", cbcKeys.EncdecDataKey);
                WriteKey($"{prefix}encdec_tweak_key.bin", cbcKeys.EncdecTweakKey);

                // Combined hdd_key.bin (24+24 = 48 bytes)
                byte[] hddKey = new byte[cbcKeys.AtaDataKey.Length + cbcKeys.AtaTweakKey.Length];
                Buffer.BlockCopy(cbcKeys.AtaDataKey, 0, hddKey, 0, cbcKeys.AtaDataKey.Length);
                Buffer.BlockCopy(cbcKeys.AtaTweakKey, 0, hddKey, cbcKeys.AtaDataKey.Length, cbcKeys.AtaTweakKey.Length);
                WriteKey($"{prefix}hdd_key.bin", hddKey);

                // Combined vflash/flash key
                byte[] flashKey = new byte[cbcKeys.EncdecDataKey.Length + cbcKeys.EncdecTweakKey.Length];
                Buffer.BlockCopy(cbcKeys.EncdecDataKey, 0, flashKey, 0, cbcKeys.EncdecDataKey.Length);
                Buffer.BlockCopy(cbcKeys.EncdecTweakKey, 0, flashKey, cbcKeys.EncdecDataKey.Length, cbcKeys.EncdecTweakKey.Length);
                WriteKey($"{prefix}flash_key.bin", flashKey);
            }

            if (doXts)
            {
                _vm.Log("NOR/Slim (XTS-128, 16-byte keys):");
                string prefix = selectedModel == 3 ? "slim_" : "";
                WriteKey($"{prefix}ata_data_key.bin", xtsKeys.AtaDataKey);
                WriteKey($"{prefix}ata_tweak_key.bin", xtsKeys.AtaTweakKey);
                WriteKey($"{prefix}encdec_data_key.bin", xtsKeys.EncdecDataKey);
                WriteKey($"{prefix}encdec_tweak_key.bin", xtsKeys.EncdecTweakKey);

                // Combined hdd_key.bin (16+16 = 32 bytes)
                byte[] hddKey = new byte[32];
                Buffer.BlockCopy(xtsKeys.AtaDataKey, 0, hddKey, 0, 16);
                Buffer.BlockCopy(xtsKeys.AtaTweakKey, 0, hddKey, 16, 16);
                WriteKey($"{prefix}hdd_key.bin", hddKey);

                // Combined vflash_key.bin
                byte[] vflashKey = new byte[32];
                Buffer.BlockCopy(xtsKeys.EncdecDataKey, 0, vflashKey, 0, 16);
                Buffer.BlockCopy(xtsKeys.EncdecTweakKey, 0, vflashKey, 16, 16);
                WriteKey($"{prefix}vflash_key.bin", vflashKey);
            }

            _vm.Log($"─── {filesWritten} key files exported to: {outDir} ───");
            _vm.StatusText = $"{filesWritten} key files exported to {outDir}";
        }
        catch (Exception ex)
        {
            _vm.StatusText = $"Export error: {ex.Message}";
            _vm.Log($"Export error: {ex}");
        }
    }

    private async void OnDecrypt(object? sender, RoutedEventArgs e)
    {
        await _vm.DecryptCommand.ExecuteAsync(null);
        
        // After successful decrypt, prompt to save if key isn't already saved
        if (_vm.IsDecrypted && !string.IsNullOrEmpty(_vm.EidRootKeyHex))
        {
            string cleanKey = _vm.EidRootKeyHex.Replace("-", "").Replace(" ", "").Trim();
            bool alreadySaved = false;
            foreach (var entry in _keyDb.Entries)
            {
                if (entry.HexKey.Replace("-", "").Replace(" ", "")
                    .Equals(cleanKey, StringComparison.OrdinalIgnoreCase))
                {
                    alreadySaved = true;
                    // Update encryption type if it was unknown
                    if (string.IsNullOrEmpty(entry.EncryptionType) && !string.IsNullOrEmpty(_vm.DetectedEncryptionType))
                    {
                        _keyDb.Add(entry.Nickname, entry.HexKey, _vm.DetectedEncryptionType);
                        _vm.Log($"Updated saved key '{entry.Nickname}' with encryption type: {_vm.DetectedEncryptionType}");
                    }
                    break;
                }
            }
            
            if (!alreadySaved)
            {
                var dialog = new Window
                {
                    Title = "Save EID Key?",
                    Width = 420,
                    Height = 200,
                    WindowStartupLocation = WindowStartupLocation.CenterOwner,
                    CanResize = false
                };
                
                var panel = new StackPanel { Margin = new global::Avalonia.Thickness(16), Spacing = 8 };
                panel.Children.Add(new TextBlock
                {
                    Text = $"Decryption successful ({_vm.DetectedEncryptionType}). Save this key for future use?",
                    TextWrapping = global::Avalonia.Media.TextWrapping.Wrap
                });
                
                var nameBox = new TextBox { Watermark = "Nickname (e.g. My Fat CECHA, Slim 3000)" };
                panel.Children.Add(nameBox);
                
                var btnRow = new StackPanel
                {
                    Orientation = global::Avalonia.Layout.Orientation.Horizontal,
                    HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Right,
                    Spacing = 8,
                    Margin = new global::Avalonia.Thickness(0, 12, 0, 0)
                };
                
                var skipBtn = new Button { Content = "Skip" };
                skipBtn.Click += (_, _) => dialog.Close();
                
                var saveBtn = new Button
                {
                    Content = "Save",
                    Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#5B6EF5")),
                    Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Colors.White)
                };
                saveBtn.Click += (_, _) =>
                {
                    string nick = nameBox.Text?.Trim() ?? "";
                    if (string.IsNullOrEmpty(nick)) nick = _vm.DetectedEncryptionType + " Console";
                    _keyDb.Add(nick, _vm.EidRootKeyHex, _vm.DetectedEncryptionType);
                    _vm.StatusText = $"Key saved as '{nick}'.";
                    _vm.Log($"EID key saved: {nick} [{_vm.DetectedEncryptionType}]");
                    dialog.Close();
                };
                
                btnRow.Children.Add(skipBtn);
                btnRow.Children.Add(saveBtn);
                panel.Children.Add(btnRow);
                
                dialog.Content = panel;
                await dialog.ShowDialog(this);
            }
        }
    }

    private async void OnExtract(object? sender, RoutedEventArgs e)
    {
        if (_vm.SelectedNode == null) return;

        if (_vm.SelectedNode.IsDirectory)
        {
            var folder = await StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
            {
                Title = "Select extraction destination",
                AllowMultiple = false
            });

            if (folder.Count > 0)
            {
                string? path = folder[0].TryGetLocalPath();
                if (path != null)
                {
                    string outputPath = Path.Combine(path, _vm.SelectedNode.Name);
                    await _vm.ExtractCommand.ExecuteAsync((_vm.SelectedNode, outputPath));
                }
            }
        }
        else
        {
            var file = await StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions
            {
                Title = "Save extracted file",
                SuggestedFileName = _vm.SelectedNode.Name
            });

            if (file != null)
            {
                string? path = file.TryGetLocalPath();
                if (path != null)
                    await _vm.ExtractCommand.ExecuteAsync((_vm.SelectedNode, path));
            }
        }
    }

    private async void OnCreateDirectory(object? sender, RoutedEventArgs e)
    {

        // Simple input dialog using a child window
        var dialog = new Window
        {
            Title = "Create Directory",
            Width = 400, Height = 150,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false
        };

        var textBox = new TextBox { Watermark = "Enter directory name...", Margin = new global::Avalonia.Thickness(12) };
        var okButton = new Button { Content = "Create", HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Right, Margin = new global::Avalonia.Thickness(12) };
        var panel = new StackPanel { Margin = new global::Avalonia.Thickness(8) };
        panel.Children.Add(new TextBlock { Text = "New directory name:", Margin = new global::Avalonia.Thickness(12, 12, 12, 4) });
        panel.Children.Add(textBox);
        panel.Children.Add(okButton);
        dialog.Content = panel;

        string? result = null;
        okButton.Click += (s, args) => { result = textBox.Text; dialog.Close(); };

        await dialog.ShowDialog(this);

        if (!string.IsNullOrWhiteSpace(result))
        {
            _vm.SetPromptFunc((title, msg) => Task.FromResult<string?>(result));
            await _vm.CreateDirectoryCommand.ExecuteAsync(null);
        }
    }

    private async void OnCopyFileToPs3(object? sender, RoutedEventArgs e)
    {

        var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = "Select file to copy to PS3",
            AllowMultiple = false,
            FileTypeFilter = new[]
            {
                new FilePickerFileType("All Files") { Patterns = new[] { "*" } }
            }
        });

        if (files.Count > 0)
        {
            string? path = files[0].TryGetLocalPath();
            if (path != null)
                await _vm.CopyFileToPs3WithPath(path);
        }
    }

    private async void OnCopyFolderToPs3(object? sender, RoutedEventArgs e)
    {
        var folders = await StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
        {
            Title = "Select folder to copy to PS3",
            AllowMultiple = false
        });

        if (folders.Count > 0)
        {
            string? path = folders[0].TryGetLocalPath();
            if (path != null)
                await _vm.CopyFolderToPs3WithPath(path);
        }
    }

    private void OnGoToRoot(object? sender, RoutedEventArgs e)
    {
        _vm.DeselectNode();
        var tree = this.FindControl<TreeView>("FileTreeView");
        if (tree != null)
            tree.UnselectAll();
    }

    private async void OnInstallPkgToHdd(object? sender, RoutedEventArgs e)
    {
        var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = "Select PKG to install to PS3 HDD",
            AllowMultiple = false,
            FileTypeFilter = new[]
            {
                new FilePickerFileType("PS3 Package Files") { Patterns = new[] { "*.pkg" } },
                new FilePickerFileType("All Files") { Patterns = new[] { "*" } }
            }
        });

        if (files.Count > 0)
        {
            string? path = files[0].TryGetLocalPath();
            if (path != null)
                await _vm.InstallPkgToHddAsync(path);
        }
    }

    private async void OnDeleteSelected(object? sender, RoutedEventArgs e)
    {
        if (_vm.SelectedNode == null) return;

        string name = _vm.SelectedNode.Name;
        string type = _vm.SelectedNode.IsDirectory ? "directory" : "file";

        // Confirmation dialog
        var dialog = new Window
        {
            Title = "Confirm Delete",
            Width = 420, Height = 160,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false
        };

        bool confirmed = false;
        var panel = new StackPanel { Margin = new global::Avalonia.Thickness(16), Spacing = 12 };
        panel.Children.Add(new TextBlock 
        { 
            Text = $"Delete {type} '{name}'?\n\nThis cannot be undone.",
            TextWrapping = global::Avalonia.Media.TextWrapping.Wrap
        });
        var btnPanel = new StackPanel { Orientation = global::Avalonia.Layout.Orientation.Horizontal, Spacing = 8, HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Right };
        var deleteBtn = new Button { Content = "Delete" };
        var cancelBtn = new Button { Content = "Cancel" };
        deleteBtn.Click += (s, a) => { confirmed = true; dialog.Close(); };
        cancelBtn.Click += (s, a) => { dialog.Close(); };
        btnPanel.Children.Add(deleteBtn);
        btnPanel.Children.Add(cancelBtn);
        panel.Children.Add(btnPanel);
        dialog.Content = panel;

        await dialog.ShowDialog(this);

        if (confirmed)
            await _vm.DeleteSelectedAsync();
    }

    private async void OnRename(object? sender, RoutedEventArgs e)
    {
        if (_vm.SelectedNode == null) return;

        string oldName = _vm.SelectedNode.Name;

        var dialog = new Window
        {
            Title = "Rename",
            Width = 400, Height = 150,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false
        };

        var textBox = new TextBox { Text = oldName, Margin = new global::Avalonia.Thickness(12) };
        var okButton = new Button { Content = "Rename", HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Right, Margin = new global::Avalonia.Thickness(12) };
        var panel = new StackPanel { Margin = new global::Avalonia.Thickness(8) };
        panel.Children.Add(new TextBlock { Text = "New name:", Margin = new global::Avalonia.Thickness(12, 12, 12, 4) });
        panel.Children.Add(textBox);
        panel.Children.Add(okButton);
        dialog.Content = panel;

        string? result = null;
        okButton.Click += (s, args) => { result = textBox.Text; dialog.Close(); };

        await dialog.ShowDialog(this);

        if (!string.IsNullOrWhiteSpace(result) && result != oldName)
            await _vm.RenameSelectedAsync(result);
    }

    private async void OnAbout(object? sender, RoutedEventArgs e)
    {
        var dialog = new Window
        {
            Title = "About",
            Width = 440,
            Height = 420,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false,
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#050508"))
        };

        // Outer container with top chrome strip
        var outer = new StackPanel { Spacing = 0 };

        // Chrome accent strip
        var strip = new Border
        {
            Height = 3,
            Margin = new global::Avalonia.Thickness(0, 0, 0, 0)
        };
        strip.Background = new global::Avalonia.Media.LinearGradientBrush
        {
            StartPoint = new global::Avalonia.RelativePoint(0, 0, global::Avalonia.RelativeUnit.Relative),
            EndPoint = new global::Avalonia.RelativePoint(1, 0, global::Avalonia.RelativeUnit.Relative),
            GradientStops =
            {
                new global::Avalonia.Media.GradientStop(global::Avalonia.Media.Color.Parse("#001030"), 0),
                new global::Avalonia.Media.GradientStop(global::Avalonia.Media.Color.Parse("#0060BB"), 0.5),
                new global::Avalonia.Media.GradientStop(global::Avalonia.Media.Color.Parse("#001030"), 1),
            }
        };
        outer.Children.Add(strip);

        var panel = new StackPanel
        {
            Margin = new global::Avalonia.Thickness(32, 28),
            Spacing = 6
        };

        // Logo: PS3
        var logoRow = new StackPanel
        {
            Orientation = global::Avalonia.Layout.Orientation.Horizontal,
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center,
            Spacing = 4,
            Margin = new global::Avalonia.Thickness(0, 0, 0, 4)
        };
        logoRow.Children.Add(new TextBlock
        {
            Text = "PS3",
            FontSize = 32,
            FontWeight = global::Avalonia.Media.FontWeight.Bold,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#005AAA")),
        });
        logoRow.Children.Add(new TextBlock
        {
            Text = " HDD TOOL",
            FontSize = 24,
            FontWeight = global::Avalonia.Media.FontWeight.Light,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#384858")),
            VerticalAlignment = global::Avalonia.Layout.VerticalAlignment.Bottom,
            Margin = new global::Avalonia.Thickness(0, 0, 0, 2)
        });
        panel.Children.Add(logoRow);

        // Version badge
        var versionBorder = new Border
        {
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#0C1828")),
            CornerRadius = new global::Avalonia.CornerRadius(10),
            Padding = new global::Avalonia.Thickness(14, 3),
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center,
            Margin = new global::Avalonia.Thickness(0, 0, 0, 12)
        };
        versionBorder.Child = new TextBlock
        {
            Text = "v1.0",
            FontSize = 11,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#406080")),
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center
        };
        panel.Children.Add(versionBorder);

        // Divider
        panel.Children.Add(new Border
        {
            Height = 1,
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#14142A")),
            Margin = new global::Avalonia.Thickness(20, 4)
        });

        // Description
        panel.Children.Add(new TextBlock
        {
            Text = "Decrypt, browse, extract, and write files to\nPS3 Fat NAND encrypted hard drives.",
            TextAlignment = global::Avalonia.Media.TextAlignment.Center,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#7888A0")),
            FontSize = 13,
            LineHeight = 22,
            Margin = new global::Avalonia.Thickness(0, 4)
        });

        // Divider
        panel.Children.Add(new Border
        {
            Height = 1,
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#14142A")),
            Margin = new global::Avalonia.Thickness(20, 4)
        });

        // Created by label
        panel.Children.Add(new TextBlock
        {
            Text = "CREATED BY",
            FontSize = 10,
            FontWeight = global::Avalonia.Media.FontWeight.Bold,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#304060")),
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center,
            Margin = new global::Avalonia.Thickness(0, 6, 0, 2)
        });

        // Author name
        panel.Children.Add(new TextBlock
        {
            Text = "Mena / Phenom Mod",
            FontSize = 20,
            FontWeight = global::Avalonia.Media.FontWeight.Bold,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#0080DD")),
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center,
            Margin = new global::Avalonia.Thickness(0, 0, 0, 6)
        });

        // Divider
        panel.Children.Add(new Border
        {
            Height = 1,
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#14142A")),
            Margin = new global::Avalonia.Thickness(20, 4)
        });

        // Tech stack
        panel.Children.Add(new TextBlock
        {
            Text = "Cross-platform  ·  .NET  ·  Avalonia",
            FontSize = 11,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#304060")),
            TextAlignment = global::Avalonia.Media.TextAlignment.Center,
            Margin = new global::Avalonia.Thickness(0, 4)
        });

        // Close button
        var okButton = new Button
        {
            Content = "Close",
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center,
            HorizontalContentAlignment = global::Avalonia.Layout.HorizontalAlignment.Center,
            Margin = new global::Avalonia.Thickness(0, 16, 0, 0),
            MinWidth = 120,
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#003D80")),
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#B0C8E8")),
            BorderBrush = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#005AAA")),
            BorderThickness = new global::Avalonia.Thickness(1),
            CornerRadius = new global::Avalonia.CornerRadius(3),
            Padding = new global::Avalonia.Thickness(0, 8)
        };
        okButton.Click += (s, args) => dialog.Close();
        panel.Children.Add(okButton);

        outer.Children.Add(panel);
        dialog.Content = outer;
        await dialog.ShowDialog(this);
    }

    private async void OnExtractPkg(object? sender, RoutedEventArgs e)
    {
        var vm = DataContext as MainViewModel;
        if (vm == null) return;

        // Pick PKG file
        var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = "Select PS3 PKG File",
            AllowMultiple = false,
            FileTypeFilter = new[]
            {
                new FilePickerFileType("PS3 PKG") { Patterns = new[] { "*.pkg" } },
                new FilePickerFileType("All Files") { Patterns = new[] { "*" } }
            }
        });

        if (files == null || files.Count == 0) return;
        string pkgPath = files[0].Path.LocalPath;

        // Pick output directory
        var folders = await StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
        {
            Title = "Select Output Directory for Extraction",
            AllowMultiple = false
        });

        if (folders == null || folders.Count == 0) return;
        string outputDir = folders[0].Path.LocalPath;

        await vm.ExtractPkgAsync(pkgPath, outputDir);
    }
}
