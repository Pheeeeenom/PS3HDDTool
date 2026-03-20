namespace PS3HddTool.Core.Crypto;

/// <summary>
/// Implements the bswap16 (byte-swap 16-bit half-words) operation required
/// before/after AES-XTS encryption/decryption of PS3 HDD sectors.
/// 
/// The PS3 uses a big-endian Cell processor, but the AES-XTS crypto operates
/// on little-endian data. Every 16-bit half-word in each sector must be 
/// byte-swapped before decryption and after encryption.
/// 
/// This is equivalent to the Linux "bswap16-ecb" cipher used by cryptsetup.
/// </summary>
public static class Bswap16
{
    /// <summary>
    /// Byte-swap every 16-bit half-word in the buffer (in-place).
    /// For each pair of bytes [A, B], swap to [B, A].
    /// </summary>
    public static void SwapInPlace(byte[] data)
    {
        for (int i = 0; i < data.Length - 1; i += 2)
        {
            (data[i], data[i + 1]) = (data[i + 1], data[i]);
        }
    }

    /// <summary>
    /// Byte-swap every 16-bit half-word, returning a new array.
    /// </summary>
    public static byte[] Swap(byte[] data)
    {
        byte[] result = new byte[data.Length];
        for (int i = 0; i < data.Length - 1; i += 2)
        {
            result[i] = data[i + 1];
            result[i + 1] = data[i];
        }
        return result;
    }

    /// <summary>
    /// Byte-swap within a Span (in-place).
    /// </summary>
    public static void SwapInPlace(Span<byte> data)
    {
        for (int i = 0; i < data.Length - 1; i += 2)
        {
            (data[i], data[i + 1]) = (data[i + 1], data[i]);
        }
    }
}
