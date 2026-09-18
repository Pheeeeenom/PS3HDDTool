using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.Primitives;
using Avalonia.Headless;
using Avalonia.Interactivity;
using Avalonia.Input;
using Avalonia.LogicalTree;
using Avalonia.Threading;
using Avalonia.VisualTree;
using PS3HddTool.Avalonia;
using PS3HddTool.Avalonia.ViewModels;
using PS3HddTool.Avalonia.Views;
using PS3HddTool.Core;

public static class TestAppBuilder
{
    public static AppBuilder BuildAvaloniaApp() => AppBuilder.Configure<PS3HddTool.Avalonia.App>()
        .UseSkia().WithInterFont().UseHeadless(new AvaloniaHeadlessPlatformOptions { UseHeadlessDrawing = false });
}

internal static class WindowTests
{
    public static async Task Run(string image, string output)
    {
        using var session = HeadlessUnitTestSession.StartNew(typeof(TestAppBuilder));
        await session.Dispatch<int>(async () =>
        {
            var recents = new RecentSourcesStore(Path.Combine(output, "window-recents"));
            var vm = new MainViewModel(new DriveProfileDatabase(Path.Combine(output, "window-profiles")), recents);
            var window = new MainWindow(vm, new EidKeyDatabase(Path.Combine(output, "window-keys.json")));
            void Check(bool value, string message) { if (!value) throw new Exception(message); }
            Button Button(string label) => window.GetLogicalDescendants().OfType<Button>().Single(b => Equals(b.Content, label));
            T Find<T>(string name) where T : Control => window.FindControl<T>(name) ?? throw new Exception($"Missing {name}.");
            void Render(string name)
            {
                Dispatcher.UIThread.RunJobs();
                AvaloniaHeadlessPlatform.ForceRenderTimerTick();
                using var frame = window.CaptureRenderedFrame();
                (frame ?? throw new Exception("Window did not render.")).Save(Path.Combine(output, name + ".png"));
            }
            void CheckScreen(bool mounted)
            {
                Dispatcher.UIThread.RunJobs();
                Check(Find<Grid>("BrowserScreen").IsVisible == mounted, "Browser visibility is stale.");
                Check(Find<ScrollViewer>("SetupScreen").IsVisible != mounted, "Setup visibility is stale.");
            }
            try
            {
                window.Show();
                CheckScreen(false);
                Check(!Find<Button>("MountButton").IsEnabled, "Empty setup enables mount.");
                Render("workbench-setup");
                Button("About").RaiseEvent(new RoutedEventArgs(global::Avalonia.Controls.Button.ClickEvent));
                Dispatcher.UIThread.RunJobs();
                var about = window.OwnedWindows.Single();
                AvaloniaHeadlessPlatform.ForceRenderTimerTick();
                using (var aboutFrame = about.CaptureRenderedFrame())
                    (aboutFrame ?? throw new Exception("About dialog did not render.")).Save(Path.Combine(output, "workbench-about.png"));
                about.Close();
                await vm.OpenImageAsync(image);
                vm.ImportPs4Key(Fixtures.Key);
                Check(vm.KeyStatusGood && vm.KeyStatusText.Contains("EAP"), "PS4 key status failed.");
                Render("workbench-ps4-setup");
                await vm.DecryptAsync();
                Check(vm.IsFilesystemMounted, vm.StatusText);
                CheckScreen(true);
                foreach (string label in new[] { "New folder", "Copy file", "Copy folder", "Install PKG", "Extract PKG", "Saved keys", "Export", "Save" })
                    Check(!Button(label).IsEnabled, $"PS4 window enables {label}.");
                foreach (string label in new[] { "New folder", "Copy file", "Copy folder", "Install PKG" })
                    Check(!Button(label).IsVisible, $"PS4 window shows {label}.");
                var combo = Find<ComboBox>("Ps4PartitionPicker");
                Check(Equals(combo.SelectedItem, vm.SelectedPs4Partition), "Partition selection binding failed.");
                var node = vm.FileTree.Single(n => n.Name == "folder");
                await vm.ExpandNodeAsync(node);
                vm.SelectedNode = node.Children.Single(n => n.Name == "hello.txt");
                Dispatcher.UIThread.RunJobs();
                Check(Button("Extract").IsEffectivelyEnabled, "Extraction button binding failed.");
                var clickedNode = vm.FileTree.Single(n => n.Name == "sparse.bin");
                var clickedItem = window.GetVisualDescendants().OfType<TreeViewItem>().Single(i => ReferenceEquals(i.DataContext, clickedNode));
                var point = clickedItem.TranslatePoint(new Point(50, clickedItem.Bounds.Height / 2), window)!.Value;
                window.MouseDown(point, MouseButton.Right);
                window.MouseUp(point, MouseButton.Right);
                Check(ReferenceEquals(vm.SelectedNode, clickedNode), "Right-click did not select the context-menu target.");
                var menu = Find<TreeView>("FileTreeView").ContextMenu!;
                MenuItem Menu(string label) => menu.Items.OfType<MenuItem>().Single(m => Equals(m.Header, label));
                menu.Open(Find<TreeView>("FileTreeView"));
                Dispatcher.UIThread.RunJobs();
                foreach (string label in new[] { "Rename…", "Delete" })
                    Check(!Menu(label).IsEnabled && !Menu(label).IsVisible, $"PS4 menu exposes {label}.");
                Check(Menu("Extract…").IsEnabled && Menu("Copy path").IsEnabled, "Read-only menu actions unavailable.");
                menu.Close();
                var folderItem = window.GetVisualDescendants().OfType<TreeViewItem>().Single(i => ReferenceEquals(i.DataContext, node));
                folderItem.IsExpanded = true;
                vm.SelectedNode = node.Children.Single(n => n.Name == "hello.txt");
                // Capture synthetic data only, never the user's console key.
                Render("ps4-window");
                window.Width = 960;
                window.Height = 660;
                Find<ToggleButton>("LogToggle").IsChecked = true;
                Render("ps4-window-narrow");
                combo.SelectedItem = vm.Ps4Partitions[1];
                CheckScreen(false);
                Check(!vm.IsFilesystemMounted && !Button("Extract").IsEnabled, "Switching left extraction active.");
                Check(Find<Button>("MountButton").IsEffectivelyEnabled, "Cannot remount after selecting a partition.");
                vm.EidRootKeyHex = new string('0', 64);
                await vm.DecryptAsync();
                CheckScreen(false);
                Check(!vm.IsFilesystemMounted, "Wrong key opens the browser.");
                vm.ImportPs4Key(Fixtures.Key);
                await vm.DecryptAsync();
                CheckScreen(true);
                vm.IsBusy = true;
                Dispatcher.UIThread.RunJobs();
                Check(!Find<Button>("MountButton").IsEnabled && !combo.IsEffectivelyEnabled &&
                      !Button("Eject").IsEffectivelyEnabled, "Busy window allows remounting/eject.");
                vm.Eject();
                Check(vm.IsFilesystemMounted, "Eject closed a busy disk.");
                vm.IsBusy = false;
                Button("Eject").RaiseEvent(new RoutedEventArgs(global::Avalonia.Controls.Button.ClickEvent));
                CheckScreen(false);
                Check(!vm.IsDiskOpen && vm.FileTree.Count == 0 && vm.SelectedNode == null, "Eject retained mount state.");
                Check(recents.Load().Count == 1 && vm.RecentSources.Count == 1, "Recent sources were not saved/deduplicated.");
                var recentButton = window.GetLogicalDescendants().OfType<Button>().Single(b => b.DataContext is RecentSourceEntry);
                recentButton.RaiseEvent(new RoutedEventArgs(global::Avalonia.Controls.Button.ClickEvent));
                for (int i = 0; i < 100 && vm.IsBusy; i++) await Task.Delay(10);
                Check(vm.IsDiskOpen && vm.IsPs4, "Recent source did not reopen the disk.");
                await vm.OpenImageAsync(Path.Combine(output, "ps3-fixture.img"));
                vm.EidRootKeyHex = "HDDKEY:" + Convert.ToHexString(Fixtures.Key);
                vm.EncryptionHint = "XTS-128";
                Check(vm.KeyStatusGood && vm.KeyStatusText.Contains("XTS"), "PS3 key status failed.");
                await vm.DecryptAsync();
                Check(vm.IsFilesystemMounted && !vm.IsPs4, "Cannot mount PS3 from the new GUI.");
                CheckScreen(true);
                vm.SelectedNode = vm.FileTree.First();
                Dispatcher.UIThread.RunJobs();
                foreach (string label in new[] { "New folder", "Copy file", "Copy folder", "Install PKG" })
                    Check(Button(label).IsVisible && Button(label).IsEnabled, $"PS3 action {label} unavailable.");
                menu.Open(Find<TreeView>("FileTreeView"));
                Dispatcher.UIThread.RunJobs();
                Check(Menu("Rename…").IsEnabled && Menu("Delete").IsEnabled, "PS3 context actions unavailable.");
                menu.Close();
                Render("workbench-ps3-narrow");
                vm.Eject();
                CheckScreen(false);
                Render("workbench-recents-narrow");
            }
            finally { window.Close(); vm.Cleanup(); }
            return 0;
        }, CancellationToken.None);
    }
}
