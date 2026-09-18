using System.Buffers.Binary;
using System.Security.Cryptography;
using PS3HddTool.Avalonia.ViewModels;
using PS3HddTool.Core;
using PS3HddTool.Core.Crypto;
using PS3HddTool.Core.Disk;
using PS3HddTool.Core.FileSystem;

internal static class Program
{
    private static int _passed;
    private static readonly string Output = Path.GetFullPath(Path.Combine("artifacts", "ps4-tests", Guid.NewGuid().ToString("N")));
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }
    private static void Throws<T>(Action action) where T : Exception
    {
        try { action(); } catch (T) { return; }
        throw new Exception($"Expected {typeof(T).Name}.");
    }
    private static void Test(string name, Action action) { action(); Console.WriteLine($"PASS {name}"); _passed++; }

    public static async Task<int> Main(string[] args)
    {
        try
        {
            Directory.CreateDirectory(Output);
            Test("AES-XTS known-answer vector", () => Check(AesXts128.SelfTest(), "XTS self-test failed."));
            foreach (bool little in new[] { false, true })
            {
                Test($"UFS2 {(little ? "PS4 LE" : "PS3 BE")} metadata, sparse and indirect extraction", () => TestUfs(little));
            }
            byte[] plain = Fixtures.Ufs(true);
            using var cipher = new AesXts128(Fixtures.Key[..16], Fixtures.Key[16..]);
            byte[] encrypted = cipher.EncryptSectors(plain, 0);
            byte[] image = Fixtures.GptDisk(encrypted);
            Test("GPT slots, checksums and bounds", () => TestGpt(image));
            Test("PS4 partition bounds, unaligned reads and every write overload", () => TestPartition(image, plain));
            Test("PS4 IV modes, key order and failed-key rejection", () => TestMount(plain, cipher));
            Test("PS3 CBC and XTS filesystem regression", TestPs3);
            Test("Malformed UFS2 and extraction destination rejection", TestInvalidUfs);
            await TestViewModel(image);
            Console.WriteLine("PASS view model import, mount, browse, extract, switching, write guards and recovery");
            _passed++;
            if (args.Length == 2) await TestRealImage(args[0], args[1]);
            else if (args.Length != 0) throw new ArgumentException("Usage: dotnet run --project PS3HddTool.Tests -- [full-disk.img keys.bin]");
            await WindowTests.Run(Path.Combine(Output, "fixture.img"), Output);
            Console.WriteLine("PASS actual window bindings, read-only controls and rendered layouts"); _passed++;
            Console.WriteLine($"PASS {_passed} test groups. Output: {Output}");
            return 0;
        }
        catch (Exception ex) { Console.Error.WriteLine(ex); return 1; }
    }

    private static void TestUfs(bool little)
    {
        using var disk = new MemoryDisk(Fixtures.Ufs(little));
        var fs = new Ufs2FileSystem(disk, 0);
        Check(fs.Mount() && fs.Superblock!.IsLittleEndian == little, "Byte order detection failed.");
        Check(fs.Superblock!.VolumeName == "Fixture", "Volume name offset failed.");
        Check(fs.Superblock.FreeSpaceBytes == 100 * 4096 + 5 * 512, "Free space parse failed.");
        Check(fs.ReadDirectory(fs.ReadInode(2)).Count == 5, "Directory parse failed.");
        Check(fs.ResolvePath("/folder/hello.txt")?.InodeNumber == 6, "Path resolution failed.");
        var sparse = fs.ReadInode(3);
        Check(sparse.ModifyTime == 1_600_000_000, "Timestamp parse failed.");
        using var output = new VerifySparseStream(sparse.Size, new()
        {
            [0] = 25, [2] = 26, [12] = 27, [14] = 29, [12 + 512] = 32
        });
        fs.ExtractInodeToStream(sparse, output);
        Check(output.Length == sparse.Size, "Sparse extraction length mismatch.");
        var triple = fs.ReadInode(4);
        using var tripleOutput = new VerifySparseStream(triple.Size, new() { [Fixtures.TripleBlock] = 0x7B });
        fs.ExtractInodeToStream(triple, tripleOutput);
        Check(tripleOutput.Length == triple.Size, "Triple indirect length mismatch.");
        Check(disk.MaxRead <= 4 * 1024 * 1024, "Read buffer is unbounded.");
        string folder = Path.Combine(Output, little ? "little" : "big");
        fs.ExtractDirectory(fs.ReadInode(5), folder);
        byte[] expected = Enumerable.Range(0, 1537).Select(i => (byte)(i % 251)).ToArray();
        Check(File.ReadAllBytes(Path.Combine(folder, "hello.txt")).SequenceEqual(expected), "Folder extraction failed.");
        Check(!File.Exists(Path.Combine(folder, "link")), "Source symlink must not be followed.");
        if (little) Throws<NotSupportedException>(() => new Ufs2Writer(fs, disk, 0, true, _ => { }));
        Check(disk.Writes == 0, "Reading caused a source write.");
    }

    private static void TestGpt(byte[] image)
    {
        using var disk = new MemoryDisk(image);
        var layout = Ps4DiskLayout.TryRead(disk)!;
        Check(layout.Partitions.Count == 2 && layout.Partitions[0].Slot == 27 && layout.Partitions[1].Slot == 29,
            "Empty GPT slots were compacted.");
        Check(layout.Partitions[0].Name == "user" && layout.Partitions[1].Name == "eap_user", "GUID mapping failed.");
        byte[] bad = (byte[])image.Clone(); bad[512 + 24] ^= 1;
        Throws<InvalidDataException>(() => Ps4DiskLayout.TryRead(new MemoryDisk(bad)));
        bad = (byte[])image.Clone(); bad[1024 + 26 * 128] ^= 1;
        Throws<InvalidDataException>(() => Ps4DiskLayout.TryRead(new MemoryDisk(bad)));
        bad = (byte[])image.Clone(); Fixtures.AddPartition(bad, 27, 2048, image.Length / 512, Fixtures.UserGuid); Fixtures.UpdateCrc(bad);
        Throws<InvalidDataException>(() => Ps4DiskLayout.TryRead(new MemoryDisk(bad)));
        Check(Ps4DiskLayout.TryRead(new MemoryDisk(new byte[512])) == null, "Small non-GPT image detection failed.");
    }

    private static void TestPartition(byte[] image, byte[] plaintext)
    {
        using var raw = new MemoryDisk(image);
        var partition = Ps4DiskLayout.TryRead(raw)!.Partitions[0];
        using (var source = new Ps4PartitionSource(raw, partition, Fixtures.Key, 0))
        {
            Check(!source.CanWrite, "PS4 view advertises writes.");
            Check(source.ReadBytes(65537, 8001).SequenceEqual(plaintext.AsSpan(65537, 8001).ToArray()), "Unaligned decrypt failed.");
            Throws<ArgumentOutOfRangeException>(() => source.ReadBytes(-1, 10));
            Throws<ArgumentOutOfRangeException>(() => source.ReadBytes(source.TotalSize - 2, 3));
            Throws<ArgumentOutOfRangeException>(() => source.ReadSectors(source.SectorCount, 1));
            Throws<NotSupportedException>(() => source.WriteBytes(0, new byte[512]));
            Throws<NotSupportedException>(() => source.WriteBytes(0, new byte[512].AsSpan()));
            Throws<NotSupportedException>(() => source.WriteSectors(0, new byte[512]));
        }
        Check(!raw.Disposed && raw.Writes == 0, "Partition view altered/closed the raw disk.");
    }

    private static void TestMount(byte[] plain, AesXts128 cipher)
    {
        foreach (long iv in new[] { 0L, 26L << 32 })
        foreach (bool reverseInput in new[] { false, true })
        {
            using var disk = new MemoryDisk(Fixtures.GptDisk(cipher.EncryptSectors(plain, iv)));
            var partition = Ps4DiskLayout.TryRead(disk)!.Partitions[0];
            byte[] key = (byte[])Fixtures.Key.Clone();
            if (reverseInput) { Array.Reverse(key, 0, 16); Array.Reverse(key, 16, 16); }
            using var mount = Ps4Mount.Open(disk, partition, key);
            Check(mount.IvOffset == iv && mount.ReversedKey == reverseInput, "IV/key detection failed.");
            Check(mount.FileSystem.ResolvePath("/folder/hello.txt") != null, "Mounted filesystem failed.");
            Throws<InvalidDataException>(() => Ps4Mount.Open(disk, partition, new byte[32]));
            Check(disk.Writes == 0, "Probing wrote to disk.");
        }
        Check(Ps4Mount.ParseKey("EAPKEY:" + Convert.ToHexString(Fixtures.Key)).SequenceEqual(Fixtures.Key), "Key parser failed.");
        Throws<FormatException>(() => Ps4Mount.ParseKey("1234"));
        Throws<FormatException>(() => Ps4Mount.ParseKey(new string('Z', 64)));
    }

    private static void TestPs3()
    {
        byte[] plain = Fixtures.Ufs(false);
        using var xts = new AesXts128(Fixtures.Key[..16], Fixtures.Key[16..]);
        using var cbc = new AesCbc192(Fixtures.Key[..24]);
        using var rawXts = new MemoryDisk(Bswap16.Swap(xts.EncryptSectors(plain, 0)));
        using var rawCbc = new MemoryDisk(Bswap16.Swap(cbc.EncryptSectors(plain)));
        using var diskXts = new DecryptedDiskSource(rawXts, Fixtures.Key[..16], Fixtures.Key[16..], true);
        using var diskCbc = new DecryptedDiskSourceCbc(rawCbc, Fixtures.Key[..24], true);
        foreach (var disk in new IDiskSource[] { diskXts, diskCbc })
        {
            var fs = new Ufs2FileSystem(disk, 0);
            Check(fs.Mount() && !fs.Superblock!.IsLittleEndian, "PS3 mount regression.");
            Check(fs.ReadInodeData(fs.ReadInode(6)).Length == 1537, "PS3 read regression.");
        }
    }

    private static void TestInvalidUfs()
    {
        var bad = Fixtures.Ufs(true);
        Fixtures.Put32(bad, 65536 + 0x34, 0, true);
        Throws<InvalidDataException>(() => new Ufs2FileSystem(new MemoryDisk(bad), 0).Mount());
        bad = Fixtures.Ufs(true); Fixtures.Put16(bad, 192 * 512 + 4, 4, true);
        var fs = new Ufs2FileSystem(new MemoryDisk(bad), 0); fs.Mount();
        Throws<InvalidDataException>(() => fs.ReadDirectory(fs.ReadInode(2)));
        foreach (string name in new[] { "../escape", "..\\escape", "/root", ".", "..", "bad\0name" })
            Throws<InvalidDataException>(() => Ufs2FileSystem.GetExtractionPath(Output, name));
        string truncated = Path.Combine(Output, "short.img"); File.WriteAllBytes(truncated, new byte[512]);
        using var source = new ImageDiskSource(truncated);
        Throws<ArgumentOutOfRangeException>(() => source.ReadBytes(500, 16));
    }

    private static async Task TestViewModel(byte[] image)
    {
        string path = Path.Combine(Output, "fixture.img");
        File.WriteAllBytes(path, image);
        byte[] before = SHA256.HashData(image);
        var vm = new MainViewModel(new DriveProfileDatabase(Path.Combine(Output, "profiles")),
            new PS3HddTool.Avalonia.RecentSourcesStore(Path.Combine(Output, "recents")));
        try
        {
            await vm.OpenImageAsync(path);
            Check(vm.IsPs4 && vm.IsDiskOpen && vm.Ps4Partitions.Count == 2 && vm.CanMount, "PS4 UI detection failed.");
            vm.ImportPs4Key(Fixtures.Key);
            await vm.DecryptAsync();
            Check(vm.IsFilesystemMounted && !vm.CanWriteFiles && !vm.CanUsePs3Tools, vm.StatusText);
            var folder = vm.FileTree.Single(n => n.Name == "folder");
            await vm.ExpandNodeAsync(folder);
            Check(folder.ChildrenLoaded && folder.Children.Count == 2, "Lazy directory browse failed.");
            var file = folder.Children.Single(n => n.Name == "hello.txt");
            vm.SelectedNode = file;
            Check(vm.CanExtract, "Extraction unavailable.");
            string extracted = Path.Combine(Output, "ui-extracted.bin");
            await vm.ExtractAsync((file, extracted));
            Check(new FileInfo(extracted).Length == 1537, vm.StatusText);
            vm.DryRunMode = false;
            await vm.CreateDirectoryAsync(); await vm.CopyFileToPs3Async();
            await vm.CopyFileToPs3WithPath("must-not-open"); await vm.CopyFolderToPs3WithPath("must-not-open");
            await vm.DeleteSelectedAsync(); await vm.RenameSelectedAsync("renamed"); await vm.InstallPkgToHddAsync("must-not-open");
            Check(SHA256.HashData(File.ReadAllBytes(path)).SequenceEqual(before), "UI changed the source image.");
            vm.SelectedPs4Partition = vm.Ps4Partitions[1];
            Check(!vm.IsFilesystemMounted && vm.SelectedNode == null && vm.FileTree.Count == 0 && !vm.CanExtract, "Stale mount after switch.");
            vm.EidRootKeyHex = new string('0', 64); await vm.DecryptAsync();
            Check(!vm.IsDecrypted && !vm.IsFilesystemMounted && vm.FileTree.Count == 0, "Wrong key reports success.");
            vm.ImportPs4Key(Fixtures.Key); await vm.DecryptAsync();
            Check(vm.IsFilesystemMounted, "Cannot recover after wrong key.");
            string ps3Path = Path.Combine(Output, "ps3-fixture.img");
            using (var xts = new AesXts128(Fixtures.Key[..16], Fixtures.Key[16..]))
                File.WriteAllBytes(ps3Path, Bswap16.Swap(xts.EncryptSectors(Fixtures.Ufs(false), 0)));
            await vm.OpenImageAsync(ps3Path);
            vm.EidRootKeyHex = "HDDKEY:" + Convert.ToHexString(Fixtures.Key);
            vm.EncryptionHint = "XTS-128";
            await vm.DecryptAsync();
            Check(!vm.IsPs4 && vm.IsFilesystemMounted && vm.CanWriteFiles && vm.Ps4Partitions.Count == 0,
                "PS4-to-PS3 switching regression: " + vm.StatusText);
            await vm.OpenImageAsync(Path.Combine(Output, "missing.img"));
            Check(!vm.IsDiskOpen && !vm.IsFilesystemMounted && !vm.IsPs4, "Failed open retained stale state.");
            await vm.OpenImageAsync(path); vm.ImportPs4Key(Fixtures.Key); await vm.DecryptAsync();
            Check(vm.IsFilesystemMounted, "Cannot reopen after failure.");
        }
        finally { vm.Cleanup(); }
    }

    private static async Task TestRealImage(string imagePath, string keyPath)
    {
        var info = new FileInfo(imagePath);
        long length = info.Length; DateTime modified = info.LastWriteTimeUtc;
        byte[] key = File.ReadAllBytes(keyPath);
        using var disk = new ImageDiskSource(imagePath);
        Check(!disk.CanWrite, "Real image opened writable.");
        var layout = Ps4DiskLayout.TryRead(disk) ?? throw new Exception("PS4 GPT missing.");
        foreach (var partition in layout.Partitions.Where(p => p.CanBrowse))
        {
            using var mount = Ps4Mount.Open(disk, partition, key);
            var fs = mount.FileSystem;
            var pending = new Queue<(long Inode, int Depth)>(); pending.Enqueue((2, 0));
            var visited = new HashSet<long>();
            var candidates = new List<Ufs2Inode>();
            int directories = 0;
            while (pending.Count > 0 && directories < 500)
            {
                var current = pending.Dequeue();
                if (!visited.Add(current.Inode) || current.Depth > 24) continue;
                var entries = fs.ReadDirectory(fs.ReadInode(current.Inode)); directories++;
                foreach (var entry in entries)
                {
                    if (entry.Name is "." or "..") continue;
                    var inode = fs.ReadInode(entry.InodeNumber);
                    if (inode.FileType == Ufs2FileType.Directory) pending.Enqueue((inode.InodeNumber, current.Depth + 1));
                    else if (inode.FileType == Ufs2FileType.RegularFile && inode.Size is > 0 and <= 32 * 1024 * 1024)
                    {
                        bool large = inode.Size > fs.Superblock!.BlockSize * 12;
                        if (!candidates.Any(n => (n.Size > fs.Superblock.BlockSize * 12) == large)) candidates.Add(inode);
                    }
                }
                if (candidates.Count == 2) break;
            }
            Check(directories > 0, "No real-image directories could be read.");
            long bytes = 0;
            foreach (var inode in candidates)
            {
                string output = Path.Combine(Output, $"real-{partition.Name}-{inode.InodeNumber}.bin");
                fs.ExtractFile(inode, output);
                byte[] referenceHash = ReferenceHash(fs, inode);
                using var extracted = File.OpenRead(output);
                Check(extracted.Length == inode.Size && SHA256.HashData(extracted).SequenceEqual(referenceHash), "Real extraction differs from independent block-by-block read.");
                bytes += inode.Size;
            }
            Console.WriteLine($"PASS real {partition.Name}: {directories} directories browsed, {candidates.Count} files / {bytes} bytes extracted and hash-verified; IV={mount.IvOffset}." +
                (candidates.Count == 0 ? " No regular files within the sample size limit; browsing verified." : ""));
            _passed++;
        }
        var vm = new MainViewModel(new DriveProfileDatabase(Path.Combine(Output, "real-profiles")),
            new PS3HddTool.Avalonia.RecentSourcesStore(Path.Combine(Output, "real-recents")));
        try
        {
            await vm.OpenImageAsync(imagePath); vm.ImportPs4Key(key); await vm.DecryptAsync();
            Check(vm.IsFilesystemMounted && vm.FileTree.Count > 0, vm.StatusText);
            vm.SelectedPs4Partition = vm.Ps4Partitions.Single(p => p.Name == "eap_user"); await vm.DecryptAsync();
            Check(vm.IsFilesystemMounted, vm.StatusText);
            vm.SelectedPs4Partition = vm.Ps4Partitions.First(p => !p.CanBrowse);
            Check(!vm.CanMount && !vm.CanWriteFiles && vm.FileTree.Count == 0, "Unsupported partition retained access.");
        }
        finally { vm.Cleanup(); CryptographicOperations.ZeroMemory(key); }
        info.Refresh();
        Check(info.Length == length && info.LastWriteTimeUtc == modified, "Source image changed.");
        Console.WriteLine("PASS real image UI mount/switch and unchanged source metadata"); _passed++;
    }

    // Independent, uncoalesced logical-block lookup for comparison with the streaming extractor.
    private static byte[] ReferenceHash(Ufs2FileSystem fs, Ufs2Inode inode)
    {
        long blockSize = fs.Superblock!.BlockSize, fanout = blockSize / 8;
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        for (long position = 0; position < inode.Size; position += blockSize)
        {
            long logical = position / blockSize;
            long pointer;
            if (logical < 12) pointer = inode.DirectBlocks[logical];
            else
            {
                long relative = logical - 12;
                int level = 1; long capacity = fanout;
                while (relative >= capacity && level < 3) { relative -= capacity; level++; capacity *= fanout; }
                pointer = level == 1 ? inode.IndirectBlock : level == 2 ? inode.DoubleIndirectBlock : inode.TripleIndirectBlock;
                while (level-- > 0 && pointer != 0)
                {
                    capacity /= fanout;
                    long index = relative / capacity; relative %= capacity;
                    byte[] value = fs.DiskSource.ReadBytes(fs.PartitionOffsetBytes + pointer * fs.Superblock.FragmentSize + index * 8, 8);
                    pointer = BinaryPrimitives.ReadInt64LittleEndian(value);
                }
            }
            int count = (int)Math.Min(blockSize, inode.Size - position);
            hash.AppendData(pointer == 0 ? new byte[count] : fs.DiskSource.ReadBytes(fs.PartitionOffsetBytes + pointer * fs.Superblock.FragmentSize, count));
        }
        return hash.GetHashAndReset();
    }
}
