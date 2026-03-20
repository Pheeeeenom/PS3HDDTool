# PS3 HDD Tool

A cross-platform desktop application for decrypting, browsing, extracting, and writing files to PS3 CECHA (Fat NAND) encrypted hard drives.

This is the first known tool capable of writing encrypted data directly to a PS3 HDD that the console accepts without triggering filesystem corruption.

---

## Features

### Decryption
- AES-CBC-192 encryption with bswap16 preprocessing for Fat NAND models (CECHA/B/C/E)
- Fast-path detection for known configurations (~1 second mount)
- Full scan fallback for unknown or non-standard setups
- EID Root Key input (48-byte key derived from console-specific data)

### Filesystem Browser
- Full UFS2 filesystem parsing: superblock, cylinder groups, inodes, directory entries
- Lazy-loading directory tree with unlimited depth traversal
- PARAM.SFO parsing: game directories display their title (e.g. "BLES80608 -- Super Motherload")
- Image preview for PNG/JPG files directly from the encrypted drive
- File details panel: size, permissions, timestamps

### Extract
- Stream-based extraction supporting files of any size (tested with 34GB+)
- Progress bar with real-time speed and ETA display
- Extract individual files or entire directory trees

### Write
- Create directories at any level of the filesystem
- Copy individual files from PC to any PS3 directory
- Copy entire folder structures recursively with progress tracking
- Double indirect block support for files up to ~64GB
- Automatic directory block expansion when existing blocks are full
- Cross-cylinder-group allocation when the local CG is full
- Fake write test mode for safe verification before committing changes

### PS3 Compatibility
All written data matches the PS3's own filesystem conventions:
- Directory permissions: 0777 (rwxrwxrwx)
- File permissions: 0666 (rw-rw-rw-)
- Correct inode fields: di_gen, di_blksize, di_blocks (allocated, not file-size-based)
- Proper timestamp layout matching FreeBSD UFS2 dinode2 structure
- CG header timestamp updates on every modification

---

## Requirements

- .NET 10 SDK (for building from source)
- Administrator/root privileges (required for physical drive access)
- PS3 EID Root Key (48 bytes: 32-byte encryption key + 16-byte IV)

## Supported Consoles

- **Confirmed working:** CECHA (60GB Fat, NAND flash, 4 USB ports)
- **Should work:** CECHB, CECHC, CECHE (other Fat NAND models using CBC-192)
- **Not yet tested:** Slim/NOR models (AES-XTS-128 -- read infrastructure exists, write not implemented)

---

## Building

### From source

```
dotnet build
dotnet run --project PS3HddTool.Avalonia
```

### Single-file executable

```
dotnet publish PS3HddTool.Avalonia -c Release -r win-x64
```

Output: `PS3HddTool.Avalonia/bin/Release/net10.0/win-x64/publish/PS3HddTool.exe`

Other platforms: replace `win-x64` with `linux-x64`, `osx-x64`, or `osx-arm64`.

---

## Usage

1. Connect the PS3 HDD to your PC via USB/SATA adapter
2. Launch PS3 HDD Tool as administrator
3. Click "Open Physical Drive" and select the PS3 drive
4. Enter your 96-character hex EID Root Key and click "Decrypt"
5. Browse the filesystem, extract files, or write new content

### Writing files to PS3

1. Navigate to the target directory in the tree (or leave unselected for root)
2. Click "Copy File to PS3" or "Copy Folder to PS3"
3. Select the file/folder from your PC
4. The tool encrypts and writes the data directly to the drive
5. Put the drive back in the PS3 -- your files will be there

### Fake write test

Enable "Fake Write Test" mode to see exactly what would be written without modifying the drive. The log panel shows every operation that would be performed, including byte offsets and data sizes.

---

## Project Structure

```
PS3HddTool/
  PS3HddTool.Core/           Core library (no UI dependency)
    Crypto/
      AesCbc192.cs            AES-CBC-192 encrypt/decrypt
      AesXts128.cs            AES-XTS-128 (Slim/NOR models)
      Bswap16.cs              16-bit byte-swap preprocessing
      Ps3KeyDerivation.cs     EID Root Key to ATA key derivation
    Disk/
      DiskSource.cs           Physical drive and image file I/O
      DecryptedDiskSourceCbc.cs   CBC-192 transparent decrypt/encrypt layer
      DecryptedDiskSource.cs      XTS-128 transparent decrypt layer
    FileSystem/
      Ufs2FileSystem.cs       UFS2 read operations (mount, inodes, directories)
      Ufs2Writer.cs           UFS2 write operations (create dir, write file, bitmaps)
      ParamSfo.cs             PARAM.SFO parser for game metadata
    Models/
      FileTreeNode.cs         Tree node model for the UI

  PS3HddTool.Avalonia/       Cross-platform GUI (Avalonia UI)
    Views/
      MainWindow.axaml        Main window layout
      MainWindow.axaml.cs     Event handlers and dialogs
    ViewModels/
      MainViewModel.cs        Application logic and commands
```

---

## Technical Details

### Encryption

PS3 Fat NAND models encrypt the entire HDD using AES-CBC-192:
- 24-byte key derived from the console's EID Root Key
- Zero IV per 512-byte sector (IV resets at each sector boundary)
- bswap16 applied before CBC decryption (byte pairs swapped)
- Write path: plaintext -> AES-CBC-192 encrypt -> bswap16 -> raw sectors

### Filesystem

The PS3 GameOS partition uses a modified FreeBSD UFS2 filesystem:
- Big-endian byte order (PowerPC Cell processor)
- 16384-byte blocks, 4096-byte fragments
- 256-byte inodes with standard UFS2 dinode2 layout
- Directory entries are variable-length records with 4-byte alignment

### Key discovery: inode field requirements

Through byte-level comparison of PS3-created vs externally-created inodes, the following fields were found to be critical for the PS3's filesystem checker to accept external writes:
- `di_mode`: 0x41FF for directories, 0x81B6 for files
- `di_gen` (offset 0x50): must be non-zero (random generation number)
- `di_blksize` (offset 0x0C): fs_bsize for directories, 0 for files
- `di_blocks` (offset 0x18): counts allocated blocks, not file size
- Timestamp offsets: 0x20 (atime), 0x28 (mtime), 0x30 (ctime), 0x38 (birthtime)
- `cg_old_time` in CG headers: must be updated on every CG modification

---

## Credits

Created by **Mena / Phenom Mod**

---

## License

This project is provided for educational and personal use. Use at your own risk. Always back up your data before writing to a PS3 HDD.

---

## Disclaimer

This tool requires a console-specific EID Root Key which can only be obtained from your own PS3. It does not bypass any copy protection or enable piracy. It is intended for managing legitimately owned content on your own hardware.
