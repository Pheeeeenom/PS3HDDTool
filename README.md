# PS3 HDD Tool

A cross-platform GUI application for decrypting and browsing PS3 hard drives using your console's EID Root Key.

Built with **.NET 8** and **Avalonia UI** — runs on Windows, macOS, and Linux.

## Features

- **Decrypt PS3 HDD** — AES-XTS-128 decryption using your EID Root Key
- **Support for disk images and physical drives** — Works with `.img`, `.bin`, `.dd`, `.raw` files or direct device access
- **Browse UFS2 filesystem** — Navigate the PS3's FreeBSD-derived file system with a tree view
- **Extract files and folders** — Export individual files or entire directory trees
- **Partition viewer** — See the PS3's disk layout and partition structure
- **Activity log** — Detailed logging of all operations

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- Your PS3 console's **EID Root Key** (32 bytes / 64 hex characters)

## Building

```bash
# Clone or extract the project
cd PS3HddTool

# Restore packages
dotnet restore

# Build
dotnet build

# Run
dotnet run --project PS3HddTool.Avalonia
```

### Publish a self-contained executable

```bash
# Windows
dotnet publish PS3HddTool.Avalonia -c Release -r win-x64 --self-contained

# Linux
dotnet publish PS3HddTool.Avalonia -c Release -r linux-x64 --self-contained

# macOS (Intel)
dotnet publish PS3HddTool.Avalonia -c Release -r osx-x64 --self-contained

# macOS (Apple Silicon)
dotnet publish PS3HddTool.Avalonia -c Release -r osx-arm64 --self-contained
```

## Usage

1. **Open a disk source**
   - Click **Open Image File** to load a disk image (`.img`, `.bin`, `.dd`, `.raw`)
   - Click **Open Physical Drive** to access a PS3 HDD connected via USB/SATA adapter

2. **Enter your EID Root Key**
   - Paste the 64-character hex string into the key field
   - Accepted formats: `AABBCC...`, `AA BB CC...`, `AA:BB:CC...`

3. **Decrypt**
   - Click **Decrypt** — the tool will derive the AES-XTS keys and attempt to mount the UFS2 filesystem
   - The partition table and filesystem structure will appear if successful

4. **Browse and extract**
   - Navigate the filesystem tree on the left
   - Select any file or folder to see its details
   - Click **Extract Selected** to save files to your computer

## How It Works

### Encryption
The PS3 encrypts its HDD using **AES-XTS-128**. The encryption keys are derived from the console's **EID Root Key** — a unique 32-byte key stored in each console's NAND/NOR flash memory.

The tool supports two key derivation paths:
- **Standard derivation**: HMAC-SHA1 of the EID Root Key halves with a fixed seed
- **Per-drive derivation**: AES-CBC decryption of encrypted ATA key data from the HDD header using the EID Root Key

### Filesystem
The PS3's GameOS partition uses **UFS2** (Unix File System 2), the same filesystem used by FreeBSD. The tool implements a read-only UFS2 parser that handles:
- Superblock parsing
- Cylinder group navigation
- Inode reading (direct, indirect, double/triple indirect blocks)
- Directory entry parsing
- File data extraction

## Project Structure

```
PS3HddTool/
├── PS3HddTool.sln                    # Solution file
├── PS3HddTool.Core/                  # Core library (no GUI dependencies)
│   ├── Crypto/
│   │   ├── AesXts128.cs              # AES-XTS-128 implementation
│   │   └── Ps3KeyDerivation.cs       # EID Root Key → encryption keys
│   ├── Disk/
│   │   ├── DiskSource.cs             # Image file & physical disk readers
│   │   ├── DecryptedDiskSource.cs    # Transparent decryption wrapper
│   │   └── Ps3DiskLayout.cs          # Partition table parser
│   ├── FileSystem/
│   │   └── Ufs2FileSystem.cs         # UFS2 filesystem implementation
│   └── Models/
│       └── FileTreeNode.cs           # UI model for file browser
├── PS3HddTool.Avalonia/              # GUI application
│   ├── ViewModels/
│   │   └── MainViewModel.cs          # Main application logic (MVVM)
│   ├── Views/
│   │   ├── MainWindow.axaml          # UI layout
│   │   └── MainWindow.axaml.cs       # Event handlers
│   ├── App.axaml / App.axaml.cs      # Avalonia application entry
│   └── Program.cs                    # Entry point
└── README.md
```

## Important Notes

- **Read-only**: This tool only reads from the disk. It never writes to your PS3 HDD. ONLY IF FAKE WRITE IS CHECKED
- **EID Root Key**: You need to obtain this from your own console (e.g., via PS3Xploit, hardware flasher, or UART dump). This tool does not extract the key.
- **Large drives**: For HDDs over 500GB, initial directory loading may take a moment as sectors are decrypted on-the-fly.
- **Key verification**: If the UFS2 superblock isn't found after decryption, the key may be incorrect or the partition layout may differ from the standard one.

## Troubleshooting

| Issue | Solution |
|-------|---------|
| "UFS2 superblock not found" | Verify your EID Root Key is correct. Try different partition offsets. |
| "Invalid EID Root Key" | Ensure the key is exactly 64 hex characters (32 bytes). |
| Physical drive not accessible | Run the application with administrator/root privileges. |
| Slow browsing | Large directories decrypt sectors on demand — first access is slower. |

## License

This tool is for personal use with your own PS3 console and hard drive.
