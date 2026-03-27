# PS3 UFS2 Filesystem Reference

## Overview

The PS3 internal HDD uses **UFS2** (Unix File System 2), a BSD filesystem originally from FreeBSD. The PS3 runs a modified FreeBSD kernel (based on ~7.x), and the on-disk format is big-endian to match the Cell processor's PowerPC architecture.

The entire HDD is encrypted. Fat/NAND PS3 models (CECHA/B/C/E) use **AES-CBC-192** with a zero IV per 512-byte sector, followed by a **byte-swap (bswap16)** on every 16-bit word. Slim/NOR models use **AES-XTS-128** with sector-based tweaks. All filesystem structures described below exist in the decrypted (plaintext) layer.

---

## Disk Layout

```
[ MBR/GPT ] [ Partition Table ] [ UFS2 Partition (dev_hdd0) ] [ Other Partitions ]
                                 |
                                 +-- Superblock (at offset 64KB into partition)
                                 +-- Cylinder Group 0
                                 +-- Cylinder Group 1
                                 +-- ...
                                 +-- Cylinder Group N
```

The UFS2 partition (`dev_hdd0`) contains the game data, user saves, system files, etc. On Fat/NAND models, the partition starts at **sector 0x20 (32)**, which is byte offset **16,384** from the start of the disk.

---

## Superblock

The superblock lives at **offset 65536 (0x10000)** from the start of the partition. It's 8192 bytes and contains all filesystem parameters.

### Key Superblock Fields

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x08 | i32 | fs_sblkno | Superblock fragment address |
| 0x0C | i32 | fs_cblkno | CG header fragment offset within each CG |
| 0x10 | i32 | fs_iblkno | Inode table fragment offset within each CG |
| 0x20 | i32 | fs_bsize | Block size (16384 bytes on PS3) |
| 0x24 | i32 | fs_fsize | Fragment size (4096 bytes on PS3) |
| 0x28 | i32 | fs_frag | Fragments per block (4 on PS3) |
| 0x54 | i32 | fs_sbsize | Superblock size in bytes |
| 0x58 | i32 | fs_nindir | Pointers per indirect block (2048) |
| 0x9C | i32 | fs_cssize | Size of CS summary table |
| 0xA0 | i32 | fs_cgsize | CG header size |
| 0xB8 | i32 | fs_ipg | Inodes per group (36992 on PS3) |
| 0xBC | i32 | fs_fpg | Fragments per group (73900 on PS3) |
| 0x3F0 | i64 | fs_cstotal.cs_ndir | Total directories |
| 0x3F8 | i64 | fs_cstotal.cs_nbfree | Total free blocks |
| 0x400 | i64 | fs_cstotal.cs_nifree | Total free inodes |
| 0x408 | i64 | fs_cstotal.cs_nffree | Total free fragments |
| 0x410 | i64 | fs_cstotal.cs_numclusters | Total free block clusters |
| 0x448 | i64 | fs_csaddr | Fragment address of CS summary table |
| 0x55C | i32 | fs_magic | Magic number (0x19540119) |

### Key Constants (PS3 500GB HDD)

- **Block size**: 16,384 bytes (16 KB)
- **Fragment size**: 4,096 bytes (4 KB)
- **Fragments per block**: 4
- **Pointers per indirect block**: 2,048 (16384 / 8 bytes per pointer)
- **Inodes per group**: 36,992
- **Fragments per group**: 73,900
- **Cylinder groups**: ~1,646 (varies by disk size)

---

## Cylinder Groups (CGs)

The filesystem is divided into cylinder groups, each containing its own inode table, data blocks, and allocation bitmaps. This locality helps performance — files are allocated near their parent directory.

### CG Header Layout

Each CG has a header that tracks allocation state:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x04 | i32 | cg_magic | Magic (0x00090255) |
| 0x08 | i32 | cg_old_time | 32-bit modification timestamp |
| 0x0C | i32 | cg_cgx | CG number |
| 0x14 | i32 | cg_ndblk | Number of data fragments in this CG |
| 0x18 | i32 | cs_ndir | Number of directories |
| 0x1C | i32 | cs_nbfree | Number of free blocks |
| 0x20 | i32 | cs_nifree | Number of free inodes |
| 0x24 | i32 | cs_nffree | Number of free fragments (partial blocks) |
| 0x28 | i32 | cg_rotor | Allocation hint (next free block search start) |
| 0x2C | i32 | cg_frotor | Fragment allocation hint |
| 0x30 | i32 | cg_irotor | Inode allocation hint |
| 0x34 | 32B | cg_frsum[8] | Free fragment run counts by size (see below) |
| 0x5C | i32 | cg_iusedoff | Offset to inode used bitmap |
| 0x60 | i32 | cg_freeoff | Offset to fragment free bitmap |
| 0x68 | i32 | cg_clustersumoff | Offset to cluster summary |
| 0x6C | i32 | cg_clusteroff | Offset to cluster bitmap |
| 0x70 | i32 | cg_nclusterblks | Number of blocks in cluster bitmap |
| 0x78 | i32 | cg_initediblk | Lazy init: next uninitialized inode block |
| 0x90 | i64 | cg_time | 64-bit UFS2 modification timestamp |

### CG Bitmaps

Each CG contains three bitmaps:

1. **Inode used bitmap** (at `cg_iusedoff`): 1 bit per inode. Bit set = inode in use.
2. **Fragment free bitmap** (at `cg_freeoff`): 1 bit per fragment. Bit set = fragment is free.
3. **Cluster bitmap** (at `cg_clusteroff`): 1 bit per block. Bit set = all fragments in block are free.

### cg_frsum — Fragment Run Summary

`cg_frsum` is an array of 8 `int32` values at offset 0x34 in the CG header. It tracks how many free fragment runs of each size exist in the CG:

- `cg_frsum[0]`: unused (always 0)
- `cg_frsum[1]`: number of free runs of exactly 1 fragment
- `cg_frsum[2]`: number of free runs of exactly 2 fragments
- `cg_frsum[3]`: number of free runs of exactly 3 fragments
- `cg_frsum[4+]`: unused on PS3 (fragsPerBlock = 4, so max partial run is 3)

Runs of size `fragsPerBlock` (4) are counted in `cs_nbfree` instead, not in `cg_frsum`.

**Example**: If a CG has a block where 1 fragment is used and 3 are free, that's one run of size 3, so `cg_frsum[3]` increments by 1.

### Cluster Summary

The cluster summary (at `cg_clustersumoff`) tracks free block cluster runs, similar to `cg_frsum` but for contiguous free blocks. This helps the allocator find large contiguous regions quickly.

---

## Inodes (dinode2)

Every file and directory is represented by a 256-byte inode (UFS2 `dinode2`). Inodes live in the inode table within each CG, starting at fragment offset `fs_iblkno`.

### Inode Layout

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | u16 | di_mode | File type and permissions |
| 0x02 | i16 | di_nlink | Link count |
| 0x04 | u32 | di_uid | Owner UID (0 = root on PS3) |
| 0x08 | u32 | di_gid | Owner GID (0 on PS3) |
| 0x0C | u32 | di_blksize | Preferred I/O block size (0 on PS3) |
| 0x10 | i64 | di_size | File size in bytes |
| 0x18 | i64 | di_blocks | 512-byte sectors allocated |
| 0x20 | i64 | di_atime | Access time (Unix epoch) |
| 0x28 | i64 | di_mtime | Modification time |
| 0x30 | i64 | di_ctime | Change time |
| 0x38 | i64 | di_birthtime | Creation time |
| 0x48 | u32 | di_gen | Generation number (NFS) |
| 0x50 | u32 | di_flags | Inode flags |
| 0x70 | 96B | di_db[12] | 12 direct block pointers (8 bytes each) |
| 0xD0 | i64 | di_ib[0] | Single indirect block pointer |
| 0xD8 | i64 | di_ib[1] | Double indirect block pointer |
| 0xE0 | i64 | di_ib[2] | Triple indirect block pointer (unused on PS3) |

### di_mode Values

- `0x41FF` — Directory, rwxrwxrwx (game directories, user-created)
- `0x41ED` — Directory, rwxr-xr-x (root directory)
- `0x41C0` — Directory, rwx------ (system directories: vm, mms, vsh, home, drm, tmp)
- `0x41C1` — Directory, rwx-----x (crash_report)
- `0x81FF` — Regular file, rwxrwxrwx (game files written by installer)
- `0x81B6` — Regular file, rw-rw-rw- (game files written natively by PS3)

The upper nibble encodes file type: `0x4xxx` = directory, `0x8xxx` = regular file, `0xAxxx` = symbolic link.

### di_blocks Calculation

`di_blocks` counts in 512-byte sectors, not filesystem blocks.

- A file using 10 full blocks: `di_blocks = 10 × (16384 / 512) = 320`
- A directory using 1 fragment: `di_blocks = 1 × (4096 / 512) = 8`
- Indirect/double-indirect metadata blocks are included in the count

### Inode Addressing

To find an inode on disk:

```
CG number     = inode_number / inodes_per_group
Index in CG   = inode_number % inodes_per_group
CG offset     = CG_number × frags_per_group × fragment_size
Inode table   = CG_offset + iblkno × fragment_size
Inode offset  = inode_table + index × 256
```

### Lazy Inode Initialization (cg_initediblk)

UFS2 uses lazy initialization — inode blocks are only zeroed on first use. The field `cg_initediblk` at offset 0x78 in the CG header tracks how many inode blocks have been initialized. When allocating an inode beyond this boundary, the inode blocks must be zeroed first and `cg_initediblk` extended.

---

## Block Pointers and the Indirect Hierarchy

This is the core of how UFS2 maps file offsets to disk locations. Every block pointer is a **64-bit fragment address** in big-endian format. To get the disk byte offset: `partition_offset + frag_address × fragment_size`.

### Direct Blocks (di_db[0..11])

The first 12 block pointers are stored directly in the inode. Each points to one 16KB data block (4 fragments).

**Capacity**: 12 blocks × 16,384 bytes = **196,608 bytes (~192 KB)**

Files ≤192KB use only direct blocks. No metadata overhead.

```
Inode
├── di_db[0]  → Block 0   (bytes 0–16383)
├── di_db[1]  → Block 1   (bytes 16384–32767)
├── ...
└── di_db[11] → Block 11  (bytes 180224–196607)
```

### Single Indirect Block (di_ib[0])

For files larger than 192KB, `di_ib[0]` points to a **single indirect block** — a 16KB block filled with 2,048 block pointers (each 8 bytes).

**Additional capacity**: 2,048 blocks × 16,384 bytes = **33,554,432 bytes (~32 MB)**

**Total with direct**: 192 KB + 32 MB ≈ **~32 MB**

```
Inode
├── di_db[0..11]  → 12 data blocks (direct)
└── di_ib[0] → Single Indirect Block (16KB)
               ├── ptr[0]    → Block 12
               ├── ptr[1]    → Block 13
               ├── ...
               └── ptr[2047] → Block 2059
```

### Double Indirect Block (di_ib[1])

For files larger than ~32MB, `di_ib[1]` points to a **double indirect block** — a 16KB block filled with up to 2,048 pointers to **L1 (level-1) indirect blocks**, each of which contains up to 2,048 data block pointers.

**Additional capacity**: 2,048 L1 blocks × 2,048 pointers × 16,384 bytes = **~64 GB**

**Total with direct + single**: `~32 MB + ~64 GB ≈ **~64 GB**`

```
Inode
├── di_db[0..11]     → 12 data blocks (direct)
├── di_ib[0] → Single Indirect Block
│              └── 2048 data block pointers
└── di_ib[1] → Double Indirect Block (L2)
               ├── L1ptr[0] → L1 Block 0
               │               ├── ptr[0]    → Data block 2060
               │               ├── ptr[1]    → Data block 2061
               │               └── ...       → (up to 2048 data blocks)
               ├── L1ptr[1] → L1 Block 1
               │               └── (2048 more data blocks)
               └── ...
```

### Triple Indirect Block (di_ib[2])

Theoretically adds another level of indirection for files larger than ~64GB. Not used on PS3 in practice.

### Which Level Does a File Use?

| File Size | Blocks Needed | Structure Used |
|-----------|---------------|----------------|
| ≤ 192 KB | ≤ 12 | Direct blocks only |
| ≤ ~32 MB | ≤ 2,060 | Direct + single indirect |
| ≤ ~64 GB | ≤ ~4,196,364 | Direct + single + double indirect |
| > ~64 GB | > ~4,196,364 | Triple indirect (theoretical) |

### Tail Fragments

For files that fit entirely in direct blocks (no indirect needed), the **last block** may be a partial allocation using fewer than 4 fragments. For example, a 50KB file needs 4 blocks (3 full + 1 partial). The last block uses `ceil(remaining_bytes / fragment_size)` fragments.

Files that use indirect blocks always allocate full blocks — no tail fragments.

---

## Directories

Directories are files whose data contains directory entries. The directory data is organized in 512-byte sections called **DIRBLKSIZ** units.

### Directory Inode

A new directory starts with:
- `di_mode = 0x41FF` (directory, rwxrwxrwx)
- `di_size = 512` (one DIRBLKSIZ section)
- `di_blocks = 8` (one 4096-byte fragment = 8 × 512-byte sectors)
- `di_nlink = 2` (self "." + parent's reference)
- `di_db[0]` = fragment address of directory data (block-aligned)

### Directory Growth

Directories grow in 512-byte DIRBLKSIZ increments within their allocated fragment:
1. New directory: 1 fragment (4096 bytes), `di_size = 512`
2. As entries fill, `di_size` grows: 1024, 1536, 2048, ..., up to 4096
3. When the fragment is full, it is reallocated to a full block (4 fragments)
4. When the block is full, a new block is allocated via `di_db[1]`, etc.

**Critical**: All unused DIRBLKSIZ sections (beyond `di_size`) must be initialized with `d_ino=0, d_reclen=512` at offset 0 of each section. Without this, directory scanners that read beyond `di_size` hit `d_reclen=0` and loop forever (causes MultiMAN crashes).

**Fragment-to-block reallocation**: When a directory outgrows its initial fragment, the adjacent 3 fragments may already be allocated to files. The allocator must check whether they are free before expanding in place. If occupied, the entire directory block must be relocated to a new block-aligned address elsewhere.

### Directory Entry Format

Each entry within a DIRBLKSIZ section:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | u32 | d_ino | Inode number (0 = deleted entry) |
| 4 | u16 | d_reclen | Record length (padded to 4-byte boundary) |
| 6 | u8 | d_type | Entry type (4=directory, 8=regular file) |
| 7 | u8 | d_namlen | Name length |
| 8 | var | d_name | Filename (null-terminated, padded to 4-byte alignment) |

The `d_reclen` field chains entries together. The last entry in each 512-byte section has its `d_reclen` extended to absorb all remaining space up to the section boundary.

### Minimum Entry Sizes

```
"."   → 12 bytes (8 header + 1 name + 1 null + 2 pad)
".."  → 12 bytes (8 header + 2 name + 1 null + 1 pad)
"EBOOT.BIN" → 20 bytes (8 header + 9 name + 1 null + 2 pad)
"all-patches.psarc" → 28 bytes (8 header + 17 name + 1 null + 2 pad)
```

Formula: `entry_size = ((8 + name_length + 1 + 3) / 4) * 4` (includes null terminator)

### Entry Ordering

The PS3 native installer writes entries in this order within each directory level:
1. `.` (self)
2. `..` (parent)
3. Regular files (type 8)
4. Subdirectories (type 4)

---

## CS Summary Table

The **cylinder summary table** is a contiguous on-disk array with one 16-byte entry per CG, stored at the fragment address `fs_csaddr`. Each entry contains:

| Offset | Size | Field |
|--------|------|-------|
| 0 | i32 | cs_ndir |
| 4 | i32 | cs_nbfree |
| 8 | i32 | cs_nifree |
| 12 | i32 | cs_nffree |

The superblock's `fs_cstotal` is the sum of all entries. Both must be consistent for `fsck` to consider the filesystem valid.

---

## Allocation Rules

### Block Allocation
- Full blocks are allocated by finding 4 consecutive free fragments at a block-aligned boundary (fragment index divisible by 4)
- The allocator prefers the same CG as the file's inode
- If the CG is full, it scans neighboring CGs

### Fragment Allocation (Directories)
- Single fragments can be allocated at any free fragment position
- Directories start with 1 fragment at a block-aligned address (so the kernel can reallocate to a full block later)

### When Allocating, Update:
1. **Fragment bitmap**: clear the bit (1→0 = free→used)
2. **cs_nbfree**: decrement if a full free block was consumed
3. **cs_nffree**: adjust for partial block changes
4. **cg_frsum**: update fragment run counts
5. **Cluster bitmap**: clear the bit if the block is no longer fully free
6. **Cluster summary**: update cluster run counts
7. **Inode bitmap**: set the bit (0→1 = unused→used) for new inodes
8. **cs_nifree**: decrement for new inodes
9. **cg_initediblk**: extend if allocating beyond lazy init boundary

### When Deallocating (Delete), Update:
1. **Fragment bitmap**: set the bit (0→1 = used→free)
2. **cs_nbfree**: increment if all fragments in a block are now free
3. **cs_nffree**: adjust for partial block changes
4. **cg_frsum**: update fragment run counts
5. **Cluster bitmap**: set the bit if the block is now fully free
6. **Cluster summary**: update cluster run counts
7. **Inode bitmap**: clear the bit (1→0 = used→unused) for freed inodes
8. **cs_nifree**: increment for freed inodes
9. **cs_ndir**: decrement when deleting directories
10. **Parent nlink**: decrement when deleting a directory (removes ".." reference)
11. **Parent directory entry**: remove entry by merging `d_reclen` into previous entry

### Deleting Files and Directories

To delete a file:
1. Walk the block pointer chain (direct → indirect → double indirect) and free all data blocks
2. Free the inode in the CG bitmap
3. Zero the inode on disk
4. Remove the directory entry from the parent

To delete a directory (recursive):
1. Read the directory entries
2. Recursively delete all children (files and subdirectories)
3. Free the directory's own data blocks and inode
4. Decrement `cs_ndir` in the CG
5. Remove the entry from the parent directory
6. Decrement the parent's `di_nlink`

---

## Bugs Found (Future Reference)

During development of the PS3 HDD Tool UFS2 writer, 22 structural bugs were discovered and fixed:

### Filesystem Metadata (caused fsck to delete data)
1. **cg_frsum never maintained** (Bug 17) — Fragment run counts stayed at zero. `fsck` detected the inconsistency and deleted all tool written data.
2. **Cluster bitmap never maintained** (Bug 17) — Free block tracking was wrong, compounding the `fsck` issue.
3. **cs_numclusters never recalculated** (Bug 17) — Superblock cluster count stale after writes.

### Directory Structure (caused PS3 crashes and corrupted filenames)
4. **Directory block 0 non-block-aligned** (Bug 15) — Fragment allocated at non-block-aligned address. When directory grew, kernel read a full block and got garbage.
5. **Unused DIRBLKSIZ sections not initialized** (Bug 18) — Sections beyond `di_size` had `reclen=0`, causing infinite loops in directory scanners (MultiMAN crash).
6. **Stale data in split directory entries** (Bug 19) — Old entry data leaked into new names (e.g., "NPUA80645" became "NPUA806456").
7. **DIRSIZ violation** (Bug 1) — Entry sizes not 4-byte aligned per DIRSIZ macro.
8. **Directory entry ordering** (Bug 12) — Files and directories interleaved instead of files-first.

### Inode and Block Allocation
9. **cg_initediblk not extended** (Bug 14) — New inodes allocated in uninitialized inode blocks contained stale data.
10. **di_blocks mismatch** (Bug 16) — Directory `di_blocks` didn't match bitmap allocation.
11. **File data block alignment** (Bug 3) — Data blocks not aligned to block boundaries.
12. **Tail allocation** (Bug 5) — Last block of direct-only files used wrong fragment count.
13. **di_blocks calculation for directories** (Bug 16) — Directories had wrong sector count after fragment-to-block reallocation.
14. **Batch allocator fragment bitmap collision** (Bug 23) — Files larger than ~33MB (those requiring double indirect blocks) would have corrupted data when read back.

### Directory Growth (caused GT6 console crashes)
15. **Case 2 read/write overflow** (Bug 21) — Growing a directory within its block assumed block 0 was always 16KB. But block 0 starts as a 4KB fragment, so this wrote 12KB into adjacent file data.
16. **Blind fragment reallocation** (Bug 22) — When expanding a directory fragment to a full block, adjacent fragments were blindly marked as used and overwritten, even when they already contained file data. Fixed by checking adjacency and relocating the block if needed.

### Data Reading/Writing
17. **Inode timestamp reader offsets** (Bug 2) — Timestamps read from wrong byte offsets.
18. **ReadInodeData fragment overread** (Bug 13) — Reader consumed more fragments than allocated.
19. **Cross-directory corruption** (Bug 10) — Block writes to one directory affected another.
20. **PKG extraction 64-bit offset truncation** (Bug 20) — PKG entry offsets read as 32-bit, causing wrong data for files past 4GB in large PKGs.
21. **game/ directory mode** (Bug 6) — Used 0x41ED instead of 0x41FF for game directories.

---

## Quick Reference: Reading a File

To read a file given its inode number:

```
1. Locate the inode (CG + offset calculation)
2. Read di_size to know the total bytes
3. Read di_db[0..11] for the first 12 data blocks
4. If di_ib[0] ≠ 0:
   a. Read the single indirect block (16KB at di_ib[0] × frag_size)
   b. Parse 2048 big-endian int64 pointers
   c. Each non-zero pointer → one 16KB data block
5. If di_ib[1] ≠ 0:
   a. Read the double indirect block (16KB)
   b. Each non-zero pointer → one L1 indirect block
   c. Each L1 block contains up to 2048 data block pointers
6. Concatenate all block data, truncate to di_size
```

All pointers are fragment addresses. Multiply by fragment_size (4096) and add partition offset to get disk byte offset.
