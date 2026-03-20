using Avalonia.Controls;
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
        // Show a dialog to select a physical drive
        var dialog = new Window
        {
            Title = "Select Physical Drive",
            Width = 500,
            Height = 350,
            WindowStartupLocation = WindowStartupLocation.CenterOwner,
            CanResize = false
        };

        var panel = new StackPanel { Margin = new global::Avalonia.Thickness(16), Spacing = 8 };

        panel.Children.Add(new TextBlock
        {
            Text = "Enter the device path for your PS3 HDD:",
            FontWeight = global::Avalonia.Media.FontWeight.SemiBold,
            Margin = new global::Avalonia.Thickness(0, 0, 0, 8)
        });

        panel.Children.Add(new TextBlock
        {
            Text = "Windows: \\\\.\\PhysicalDrive1  (check Disk Management)\n" +
                   "Linux: /dev/sdb  (check lsblk)\n" +
                   "macOS: /dev/disk2  (check diskutil list)",
            Foreground = new global::Avalonia.Media.SolidColorBrush(global::Avalonia.Media.Color.Parse("#999")),
            FontSize = 12,
            Margin = new global::Avalonia.Thickness(0, 0, 0, 12)
        });

        var pathBox = new TextBox
        {
            Watermark = "Device path...",
            FontFamily = new global::Avalonia.Media.FontFamily("Consolas,Courier New,monospace")
        };
        panel.Children.Add(pathBox);

        panel.Children.Add(new TextBlock
        {
            Text = "Disk size (bytes, 0 = auto-detect):",
            Margin = new global::Avalonia.Thickness(0, 8, 0, 0)
        });

        var sizeBox = new TextBox { Text = "0", Watermark = "0" };
        panel.Children.Add(sizeBox);

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
            if (!string.IsNullOrWhiteSpace(pathBox.Text))
            {
                long size = 0;
                long.TryParse(sizeBox.Text, out size);
                dialog.Close();
                await _vm.OpenPhysicalDriveCommand.ExecuteAsync((pathBox.Text, size));
            }
        };

        buttonPanel.Children.Add(cancelBtn);
        buttonPanel.Children.Add(okBtn);
        panel.Children.Add(buttonPanel);

        dialog.Content = panel;
        await dialog.ShowDialog(this);
    }

    private async void OnDecrypt(object? sender, RoutedEventArgs e)
    {
        await _vm.DecryptCommand.ExecuteAsync(null);
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
}
