namespace InfoHashFinder.Models;

public sealed class InfoHashRecord
{
	public byte[] InfoHash { get; init; }
	public DateTimeOffset FirstSeen { get; init; }

	public InfoHashRecord(byte[] InfoHash, DateTimeOffset FirstSeen)
	{
		this.InfoHash = InfoHash;
		this.FirstSeen = FirstSeen;
	}

	// Parameterless constructor for Dapper
	public InfoHashRecord()
	{
		InfoHash = [];
		FirstSeen = DateTimeOffset.UtcNow;
	}
}