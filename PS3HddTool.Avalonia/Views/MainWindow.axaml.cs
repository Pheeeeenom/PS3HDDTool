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

    private void OnDrop(object? sender, DragEventArgs e)
    {
        var fileNames = e.Data.GetFiles();
        if (fileNames == null) return;
        
        foreach (var item in fileNames)
        {
            string? path = item.TryGetLocalPath();
            if (path == null) continue;
            
            string ext = System.IO.Path.GetExtension(path).ToLowerInvariant();
            
            if (ext == ".bin" || ext == ".eid")
            {
                try
                {
                    byte[] data = System.IO.File.ReadAllBytes(path);
                    
                    // EID key is 48 bytes raw, or 96 hex chars in a text file
                    string hexKey;
                    if (data.Length == 48)
                    {
                        // Raw binary — 48 bytes
                        hexKey = BitConverter.ToString(data).Replace("-", "");
                    }
                    else if (data.Length >= 96 && data.Length <= 200)
                    {
                        // Possibly hex-encoded text
                        string text = System.Text.Encoding.ASCII.GetString(data).Trim();
                        text = text.Replace("-", "").Replace(" ", "").Replace("\r", "").Replace("\n", "");
                        if (text.Length == 96 && System.Text.RegularExpressions.Regex.IsMatch(text, "^[0-9a-fA-F]+$"))
                            hexKey = text;
                        else
                        {
                            _vm.StatusText = $"File '{System.IO.Path.GetFileName(path)}' is not a valid EID key (wrong format).";
                            return;
                        }
                    }
                    else
                    {
                        _vm.StatusText = $"File '{System.IO.Path.GetFileName(path)}' is not a valid EID key ({data.Length} bytes, expected 48).";
                        return;
                    }
                    
                    _vm.EidRootKeyHex = hexKey;
                    _vm.StatusText = $"EID key loaded from {System.IO.Path.GetFileName(path)}";
                    _vm.Log($"EID key loaded via drag-drop: {System.IO.Path.GetFileName(path)}");
                }
                catch (Exception ex)
                {
                    _vm.StatusText = $"Error reading key file: {ex.Message}";
                }
                return;
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
        if (string.IsNullOrWhiteSpace(currentKey) || currentKey.Replace("-", "").Replace(" ", "").Length != 96)
        {
            _vm.StatusText = "Enter a valid 96-character EID Root Key first.";
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
            string display = $"{entry.Nickname}{encLabel}  —  {entry.HexKey[..16]}...  ({entry.DateAdded:yyyy-MM-dd})";
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
            Title = "About PS3 HDD Tool",
            Width = 460,
            Height = 430,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false,
            Background = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#1E1E2E"))
        };

        var panel = new StackPanel
        {
            Margin = new global::Avalonia.Thickness(24),
            Spacing = 12
        };

        panel.Children.Add(new TextBlock
        {
            Text = "PS3 HDD Tool",
            FontSize = 28,
            FontWeight = global::Avalonia.Media.FontWeight.Bold,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#5B6EF5")),
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center
        });

        panel.Children.Add(new TextBlock
        {
            Text = "v1.0",
            FontSize = 14,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#999")),
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center
        });

        panel.Children.Add(new Separator { Height = 1, Margin = new global::Avalonia.Thickness(0, 4) });

        panel.Children.Add(new TextBlock
        {
            Text = "Decrypt, browse, extract, and write files to\nPS3 CECHA Fat NAND encrypted HDDs.",
            TextAlignment = global::Avalonia.Media.TextAlignment.Center,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#CCC")),
            LineHeight = 22
        });

        panel.Children.Add(new Separator { Height = 1, Margin = new global::Avalonia.Thickness(0, 4) });

        panel.Children.Add(new TextBlock
        {
            Text = "Created by",
            FontSize = 12,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#888")),
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center
        });

        panel.Children.Add(new TextBlock
        {
            Text = "Mena / Phenom Mod",
            FontSize = 20,
            FontWeight = global::Avalonia.Media.FontWeight.Bold,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#50FA7B")),
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center
        });

        panel.Children.Add(new Separator { Height = 1, Margin = new global::Avalonia.Thickness(0, 4) });

        panel.Children.Add(new TextBlock
        {
            Text = "Cross-platform (.NET/Avalonia)",
            FontSize = 11,
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#666")),
            TextAlignment = global::Avalonia.Media.TextAlignment.Center,
            LineHeight = 18
        });

        var okButton = new Button
        {
            Content = "Close",
            HorizontalAlignment = global::Avalonia.Layout.HorizontalAlignment.Center,
            Margin = new global::Avalonia.Thickness(0, 8, 0, 0),
            MinWidth = 100
        };
        okButton.Click += (s, args) => dialog.Close();
        panel.Children.Add(okButton);

        dialog.Content = panel;
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
