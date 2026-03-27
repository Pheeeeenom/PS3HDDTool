using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text.Json;

namespace PS3HddTool.Core;

/// <summary>
/// Caches successful decryption parameters per drive, keyed by a fingerprint
/// of the raw (encrypted) sector 0. This allows instant re-decryption without
/// brute-forcing all key/bswap/offset combinations on subsequent opens.
/// </summary>
public class DriveProfileDatabase
{
    private readonly string _filePath;
    private List<DriveProfile> _profiles = new();

    public IReadOnlyList<DriveProfile> Profiles => _profiles.AsReadOnly();

    public DriveProfileDatabase()
    {
        string appData = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "PS3HddTool");
        Directory.CreateDirectory(appData);
        _filePath = Path.Combine(appData, "drive_profiles.json");
        Load();
    }

    /// <summary>
    /// Compute a fingerprint from raw (encrypted) sector 0 data.
    /// Uses SHA-256 of the first 512 bytes.
    /// </summary>
    public static string ComputeFingerprint(byte[] rawSector0)
    {
        byte[] hash = SHA256.HashData(rawSector0.AsSpan(0, Math.Min(512, rawSector0.Length)));
        return Convert.ToHexString(hash);
    }

    /// <summary>
    /// Look up a cached profile by fingerprint.
    /// </summary>
    public DriveProfile? Find(string fingerprint)
    {
        foreach (var p in _profiles)
        {
            if (p.Fingerprint.Equals(fingerprint, StringComparison.OrdinalIgnoreCase))
                return p;
        }
        return null;
    }

    /// <summary>
    /// Save or update a drive profile after successful decryption.
    /// </summary>
    public void Save(DriveProfile profile)
    {
        // Remove existing entry for same fingerprint
        _profiles.RemoveAll(p =>
            p.Fingerprint.Equals(profile.Fingerprint, StringComparison.OrdinalIgnoreCase));

        profile.LastUsed = DateTime.UtcNow;
        _profiles.Add(profile);
        Persist();
    }

    /// <summary>
    /// Remove a cached profile.
    /// </summary>
    public void Remove(string fingerprint)
    {
        _profiles.RemoveAll(p =>
            p.Fingerprint.Equals(fingerprint, StringComparison.OrdinalIgnoreCase));
        Persist();
    }

    private void Load()
    {
        try
        {
            if (File.Exists(_filePath))
            {
                string json = File.ReadAllText(_filePath);
                _profiles = JsonSerializer.Deserialize<List<DriveProfile>>(json) ?? new();
            }
        }
        catch
        {
            _profiles = new();
        }
    }

    private void Persist()
    {
        try
        {
            var options = new JsonSerializerOptions { WriteIndented = true };
            string json = JsonSerializer.Serialize(_profiles, options);
            File.WriteAllText(_filePath, json);
        }
        catch { }
    }
}

public class DriveProfile
{
    /// <summary>SHA-256 hex of raw encrypted sector 0.</summary>
    public string Fingerprint { get; set; } = "";

    /// <summary>"CBC-192" or "XTS-128"</summary>
    public string EncryptionType { get; set; } = "";

    /// <summary>Whether bswap16 is applied before decryption.</summary>
    public bool Bswap16 { get; set; }

    /// <summary>Sector offset where the UFS2 partition starts.</summary>
    public long PartitionSector { get; set; }

    /// <summary>Hex-encoded data key (24 bytes for CBC, 16 bytes for XTS).</summary>
    public string DataKeyHex { get; set; } = "";

    /// <summary>Hex-encoded tweak key (XTS only, 16 bytes). Empty for CBC.</summary>
    public string TweakKeyHex { get; set; } = "";

    /// <summary>Human-readable label.</summary>
    public string Label { get; set; } = "";

    /// <summary>Drive size in bytes (informational).</summary>
    public long DriveSizeBytes { get; set; }

    public DateTime LastUsed { get; set; } = DateTime.UtcNow;

    public override string ToString() =>
        $"{Label} [{EncryptionType}] ({DriveSizeBytes / (1024 * 1024 * 1024.0):F0} GB)";
}
