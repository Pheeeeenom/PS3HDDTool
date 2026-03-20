using System.Security.Cryptography;

namespace PS3HddTool.Core.Crypto;

/// <summary>
/// Derives AES-XTS HDD encryption keys from the PS3 EID Root Key.
/// 
/// All retail PS3 models (Fat NAND, Fat NOR, Slim) use the SAME seeds.
/// The only difference is key length:
///   - Fat NAND: AES-CBC-192 (uses 24 bytes of each ATA key)
///   - Fat NOR / Slim: AES-XTS-128 (uses 16 bytes of each ATA key)
/// 
/// Process (matching ps3hdd_keygen.sh exactly):
///   1. EID Root Key is 48 bytes: bytes[0..31] = AES-256 key, bytes[32..47] = IV
///   2. Fixed 32-byte seeds are ENCRYPTED with AES-256-CBC(erk_key, erk_iv)
///   3. First 16 bytes of each result = XTS key (or 24 bytes for CBC-192)
///   4. hdd_key.bin = ata_data_key[0..15] + ata_tweak_key[0..15]  (for XTS)
///   5. vflash_key.bin = encdec_data_key[0..15] + encdec_tweak_key[0..15]
///   6. Before XTS decryption, sectors need bswap16 byte-swapping
/// 
/// Seeds verified against:
///   github.com/einsteinx2/PS3-Reclaim-HDD-Space/blob/master/ps3hdd_keygen.sh
/// </summary>
public static class Ps3KeyDerivation
{
    // ─── Universal seeds (same for Fat NAND, Fat NOR, Slim, early Slim) ──
    // From ps3hdd_keygen.sh options 1, 2, and 3 — they all use these seeds.

    private static readonly byte[] AtaDataSeed =
        HexToBytes("D92D65DB057D49E1A66F2274B8BAC50883844ED756CA79516362EA8ADAC60326");

    private static readonly byte[] AtaTweakSeed =
        HexToBytes("C3B3B5AACC74CD6A48EFABF44DCDF16E379F55F5777D09FBEEDE07058E94BE08");

    private static readonly byte[] EncdecDataSeed =
        HexToBytes("E2D05D4071945B01C36D5151E88CB8334AAA298081D8C44F185DC660ED575686");

    private static readonly byte[] EncdecTweakSeed =
        HexToBytes("02083292C305D538BC50E699710C0A3E55F51CBAA535A38030B67F79C905BDA3");

    /// <summary>
    /// Derive HDD (ATA) and VFLASH (ENCDEC) keys for XTS-128 mode.
    /// Used by Fat NOR and Slim consoles.
    /// </summary>
    public static Ps3DerivedKeys DeriveKeys(byte[] eidRootKey)
    {
        ValidateKey(eidRootKey);
        byte[] erkKey = eidRootKey[..32];
        byte[] erkIv = eidRootKey[32..48];

        return new Ps3DerivedKeys
        {
            AtaDataKey = AesCbc256Encrypt(AtaDataSeed, erkKey, erkIv)[..16],
            AtaTweakKey = AesCbc256Encrypt(AtaTweakSeed, erkKey, erkIv)[..16],
            EncdecDataKey = AesCbc256Encrypt(EncdecDataSeed, erkKey, erkIv)[..16],
            EncdecTweakKey = AesCbc256Encrypt(EncdecTweakSeed, erkKey, erkIv)[..16]
        };
    }

    /// <summary>
    /// Derive HDD keys for CBC-192 mode. Used by Fat NAND consoles.
    /// Returns 24-byte keys instead of 16-byte.
    /// </summary>
    public static Ps3DerivedKeys DeriveKeysFatNand(byte[] eidRootKey)
    {
        ValidateKey(eidRootKey);
        byte[] erkKey = eidRootKey[..32];
        byte[] erkIv = eidRootKey[32..48];

        return new Ps3DerivedKeys
        {
            AtaDataKey = AesCbc256Encrypt(AtaDataSeed, erkKey, erkIv)[..24],
            AtaTweakKey = AesCbc256Encrypt(AtaTweakSeed, erkKey, erkIv)[..24],
            EncdecDataKey = AesCbc256Encrypt(EncdecDataSeed, erkKey, erkIv)[..16],
            EncdecTweakKey = AesCbc256Encrypt(EncdecTweakSeed, erkKey, erkIv)[..16]
        };
    }

    /// <summary>
    /// Try all possible key derivation approaches for scanning.
    /// </summary>
    public static List<(string Method, byte[] DataKey, byte[] TweakKey)> DeriveAllPossibleKeys(byte[] eidRootKey)
    {
        ValidateKey(eidRootKey);
        byte[] erkKey = eidRootKey[..32];
        byte[] erkIv = eidRootKey[32..48];

        var results = new List<(string, byte[], byte[])>();

        // Method 1: ATA keys, 16 bytes (Fat NOR / Slim / XTS-128)
        {
            byte[] dk = AesCbc256Encrypt(AtaDataSeed, erkKey, erkIv)[..16];
            byte[] tk = AesCbc256Encrypt(AtaTweakSeed, erkKey, erkIv)[..16];
            results.Add(("ATA XTS-128 (16B, NOR/Slim)", dk, tk));
        }

        // Method 2: ATA keys, 24 bytes truncated to 16 for XTS attempt (Fat NAND compat)
        // Note: Fat NAND actually uses CBC-192, but we try XTS with first 16 bytes anyway
        {
            byte[] dk = AesCbc256Encrypt(AtaDataSeed, erkKey, erkIv)[..16];
            byte[] tk = AesCbc256Encrypt(AtaTweakSeed, erkKey, erkIv)[..16];
            // Same as method 1 for XTS, but let's also try reversed key order
            results.Add(("ATA XTS-128 reversed (tweak as data)", tk, dk));
        }

        // Method 3: ENCDEC keys (for VFLASH decryption)
        {
            byte[] dk = AesCbc256Encrypt(EncdecDataSeed, erkKey, erkIv)[..16];
            byte[] tk = AesCbc256Encrypt(EncdecTweakSeed, erkKey, erkIv)[..16];
            results.Add(("ENCDEC XTS-128 (VFLASH)", dk, tk));
        }

        // Method 4: Try using the raw ERK bytes directly as keys (in case it's pre-derived)
        {
            results.Add(("Direct ERK bytes 0-15/16-31", erkKey[..16], erkKey[16..32]));
        }

        // Method 5: Try raw ERK reversed halves
        {
            results.Add(("Direct ERK bytes 16-31/0-15", erkKey[16..32], erkKey[..16]));
        }

        return results;
    }

    private static void ValidateKey(byte[] eidRootKey)
    {
        if (eidRootKey.Length != 48)
            throw new ArgumentException(
                $"EID Root Key must be exactly 48 bytes (96 hex chars). Got {eidRootKey.Length} bytes.");
    }

    private static byte[] AesCbc256Encrypt(byte[] data, byte[] key, byte[] iv)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        aes.IV = iv;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.None;

        using var encryptor = aes.CreateEncryptor();
        return encryptor.TransformFinalBlock(data, 0, data.Length);
    }

    public static byte[] ParseEidRootKey(string hexString)
    {
        string cleaned = hexString
            .Replace(" ", "").Replace(":", "").Replace("-", "")
            .Replace("0x", "").Replace(",", "").Trim();

        if (cleaned.Length != 96)
            throw new ArgumentException(
                $"EID Root Key must be 96 hex characters (48 bytes). Got {cleaned.Length} characters.");

        byte[] key = new byte[48];
        for (int i = 0; i < 48; i++)
            key[i] = Convert.ToByte(cleaned.Substring(i * 2, 2), 16);

        return key;
    }

    public static string DescribeKey(byte[] eidRootKey)
    {
        return $"{eidRootKey.Length}-byte EID Root Key (key=32B + iv=16B)";
    }

    private static byte[] HexToBytes(string hex)
    {
        byte[] bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
            bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
        return bytes;
    }
}

public class Ps3DerivedKeys
{
    public byte[] AtaDataKey { get; set; } = Array.Empty<byte>();
    public byte[] AtaTweakKey { get; set; } = Array.Empty<byte>();
    public byte[] EncdecDataKey { get; set; } = Array.Empty<byte>();
    public byte[] EncdecTweakKey { get; set; } = Array.Empty<byte>();
}
