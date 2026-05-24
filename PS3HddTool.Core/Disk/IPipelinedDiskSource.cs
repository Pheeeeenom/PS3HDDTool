namespace PS3HddTool.Core.Disk;

public interface IPipelinedDiskSource : IDiskSource
{
    void EncryptForWrite(byte[] plaintext, int byteCount, long offset, Span<byte> ciphertext);

    IDiskSource RawSource { get; }
}