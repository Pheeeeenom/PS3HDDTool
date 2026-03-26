using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace PS3HddTool.Core;

/// <summary>
/// Simple JSON-based database for storing EID Root Keys with nicknames.
/// Stored in user's app data directory.
/// </summary>
public class EidKeyDatabase
{
    private readonly string _filePath;
    private List<EidKeyEntry> _entries = new();

    public IReadOnlyList<EidKeyEntry> Entries => _entries.AsReadOnly();

    public EidKeyDatabase()
    {
        string appData = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "PS3HddTool");
        Directory.CreateDirectory(appData);
        _filePath = Path.Combine(appData, "eid_keys.json");
        Load();
    }

    public EidKeyDatabase(string filePath)
    {
        _filePath = filePath;
        Load();
    }

    public void Add(string nickname, string hexKey, string encryptionType = "")
    {
        // Validate: either 96-char EID root key hex, or prefixed pre-derived key
        string clean = hexKey.Replace("-", "").Replace(" ", "").Trim();
        bool isEidRoot = clean.Length == 96 && !clean.Contains(":");
        bool isPrefixed = clean.StartsWith("HDDKEY:", StringComparison.OrdinalIgnoreCase)
                       || clean.StartsWith("CBCKEY:", StringComparison.OrdinalIgnoreCase);

        if (!isEidRoot && !isPrefixed)
            throw new ArgumentException(
                $"Key must be 96 hex chars (EID root key) or prefixed HDDKEY:/CBCKEY: format. Got: {clean.Length} chars.");

        string compareKey = clean.ToUpperInvariant();

        // Check for duplicate key
        foreach (var e in _entries)
        {
            if (e.HexKey.Replace("-", "").Replace(" ", "").Equals(compareKey, StringComparison.OrdinalIgnoreCase))
            {
                // Update nickname and encryption type if key already exists
                e.Nickname = nickname;
                if (!string.IsNullOrEmpty(encryptionType))
                    e.EncryptionType = encryptionType;
                Save();
                return;
            }
        }

        _entries.Add(new EidKeyEntry
        {
            Nickname = nickname,
            HexKey = clean,
            EncryptionType = encryptionType,
            DateAdded = DateTime.UtcNow
        });
        Save();
    }

    public void Remove(string hexKey)
    {
        string clean = hexKey.Replace("-", "").Replace(" ", "").Trim();
        _entries.RemoveAll(e => 
            e.HexKey.Replace("-", "").Replace(" ", "").Equals(clean, StringComparison.OrdinalIgnoreCase));
        Save();
    }

    public void UpdateNickname(int index, string newNickname)
    {
        if (index >= 0 && index < _entries.Count)
        {
            _entries[index].Nickname = newNickname;
            Save();
        }
    }

    private void Load()
    {
        try
        {
            if (File.Exists(_filePath))
            {
                string json = File.ReadAllText(_filePath);
                _entries = JsonSerializer.Deserialize<List<EidKeyEntry>>(json) ?? new();
            }
        }
        catch
        {
            _entries = new();
        }
    }

    private void Save()
    {
        try
        {
            var options = new JsonSerializerOptions { WriteIndented = true };
            string json = JsonSerializer.Serialize(_entries, options);
            File.WriteAllText(_filePath, json);
        }
        catch { }
    }
}

public class EidKeyEntry
{
    public string Nickname { get; set; } = "";
    public string HexKey { get; set; } = "";
    public string EncryptionType { get; set; } = ""; // "CBC-192" or "XTS-128" or "" (unknown)
    public DateTime DateAdded { get; set; } = DateTime.UtcNow;

    public override string ToString()
    {
        string displayKey = HexKey;
        string typeTag = "";
        if (displayKey.StartsWith("HDDKEY:", StringComparison.OrdinalIgnoreCase))
        {
            displayKey = displayKey.Substring(7);
            typeTag = " [XTS]";
        }
        else if (displayKey.StartsWith("CBCKEY:", StringComparison.OrdinalIgnoreCase))
        {
            displayKey = displayKey.Substring(7);
            typeTag = " [CBC]";
        }

        string shortKey = displayKey.Length >= 8 ? displayKey[..8] + "..." : displayKey;
        return string.IsNullOrEmpty(Nickname)
            ? shortKey + typeTag
            : $"{Nickname}{typeTag} ({shortKey})";
    }
}
