namespace InfoHashFinder.Models;

public sealed class NodeRecord
{
	public string Address { get; init; }
	public int Port { get; init; }
	public DateTimeOffset LastSeen { get; init; }

	public NodeRecord(string Address, int Port, DateTimeOffset LastSeen)
	{
		this.Address = Address;
		this.Port = Port;
		this.LastSeen = LastSeen;
	}

	// Parameterless constructor for Dapper
	public NodeRecord()
	{
		Address = string.Empty;
		Port = 0;
		LastSeen = DateTimeOffset.UtcNow;
	}
}