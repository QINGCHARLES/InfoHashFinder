using Dapper;
using InfoHashFinder.Models;
using Microsoft.Data.Sqlite;

namespace InfoHashFinder.Persistence;

public sealed class Repository(string? ConnectionString = null)
{
	private const string DefaultConnection = "Data Source=dht.db;Pooling=true;";

	private const string SchemaSql =
		"""
		PRAGMA journal_mode = WAL;

		CREATE TABLE IF NOT EXISTS InfoHashes
		(
			InfoHash BLOB PRIMARY KEY,
			FirstSeen TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS Nodes
		(
			Address  TEXT NOT NULL,
			Port     INTEGER NOT NULL,
			LastSeen TEXT NOT NULL,
			PRIMARY KEY(Address, Port)
		);

		CREATE TABLE IF NOT EXISTS DhtMessageStats
		(
			MessageType TEXT PRIMARY KEY,
			Count INTEGER NOT NULL DEFAULT 0,
			LastSeen TEXT NOT NULL
		);
		""";

	private readonly string ConnectionStringValue = ConnectionString ?? DefaultConnection;

	static Repository()
	{
		// Register custom DateTimeOffset handler exactly once.
		SqlMapper.AddTypeHandler(new DateTimeOffsetHandler());
	}

	public async Task EnsureSchemaAsync()
	{
		await using SqliteConnection Connection = new(ConnectionStringValue);
		await Connection.OpenAsync();
		await Connection.ExecuteAsync(SchemaSql);
	}

	private async Task<SqliteConnection> CreateConnectionAsync()
	{
		SqliteConnection Connection = new(ConnectionStringValue);
		await Connection.OpenAsync();
		return Connection;
	}

	public async Task UpsertInfoHashAsync(InfoHashRecord InfoHash)
	{
		const string Sql =
			"""
			INSERT OR IGNORE INTO InfoHashes(InfoHash, FirstSeen)
			VALUES(@InfoHash, @FirstSeen);
			""";

		await using SqliteConnection Connection = await CreateConnectionAsync();
		await Connection.ExecuteAsync(Sql, InfoHash);
	}

	public async Task UpsertNodeAsync(NodeRecord Node)
	{
		const string Sql =
			"""
			INSERT INTO Nodes(Address, Port, LastSeen)
			VALUES(@Address, @Port, @LastSeen)
			ON CONFLICT(Address, Port)
			DO UPDATE SET LastSeen = excluded.LastSeen;
			""";

		await using SqliteConnection Connection = await CreateConnectionAsync();
		await Connection.ExecuteAsync(Sql, Node);
	}

	public async Task IncrementMessageTypeAsync(string MessageType)
	{
		const string Sql =
			"""
			INSERT INTO DhtMessageStats(MessageType, Count, LastSeen)
			VALUES(@MessageType, 1, @LastSeen)
			ON CONFLICT(MessageType)
			DO UPDATE SET Count = Count + 1, LastSeen = excluded.LastSeen;
			""";

		await using SqliteConnection Connection = await CreateConnectionAsync();
		await Connection.ExecuteAsync(Sql, new { MessageType, LastSeen = DateTimeOffset.UtcNow.ToString("O") });
	}

	public async Task<IEnumerable<(string MessageType, int Count, DateTimeOffset LastSeen)>> GetMessageStatsAsync()
	{
		const string Sql = "SELECT MessageType, Count, LastSeen FROM DhtMessageStats ORDER BY Count DESC;";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		var Results = await Connection.QueryAsync<(string MessageType, int Count, string LastSeen)>(Sql);
		return Results.Select(r => (r.MessageType, r.Count, DateTimeOffset.Parse(r.LastSeen)));
	}

	public async Task<IEnumerable<NodeRecord>> LoadNodesAsync()
	{
		const string Sql = "SELECT Address, Port, LastSeen FROM Nodes;";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		return await Connection.QueryAsync<NodeRecord>(Sql);
	}

	public async Task<int> GetInfoHashCountAsync()
	{
		const string Sql = "SELECT COUNT(*) FROM InfoHashes;";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		return await Connection.QuerySingleAsync<int>(Sql);
	}

	public async Task<int> GetNodeCountAsync()
	{
		const string Sql = "SELECT COUNT(*) FROM Nodes;";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		return await Connection.QuerySingleAsync<int>(Sql);
	}

	public async Task<int> GetActiveNodesInLastHourAsync()
	{
		const string Sql = """
			SELECT COUNT(*) 
			FROM Nodes 
			WHERE LastSeen >= datetime('now', '-1 hour');
			""";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		return await Connection.QuerySingleAsync<int>(Sql);
	}

	public async Task<IEnumerable<InfoHashRecord>> GetRecentInfoHashesAsync(int Limit = 10)
	{
		const string Sql = """
			SELECT InfoHash, FirstSeen 
			FROM InfoHashes 
			ORDER BY FirstSeen DESC 
			LIMIT @Limit;
			""";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		return await Connection.QueryAsync<InfoHashRecord>(Sql, new { Limit });
	}

	public async Task<int> GetInfoHashesInLastHourAsync()
	{
		const string Sql = """
			SELECT COUNT(*) 
			FROM InfoHashes 
			WHERE FirstSeen >= datetime('now', '-1 hour');
			""";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		return await Connection.QuerySingleAsync<int>(Sql);
	}

	public async Task<int> GetInfoHashesInLastDayAsync()
	{
		const string Sql = """
			SELECT COUNT(*) 
			FROM InfoHashes 
			WHERE FirstSeen >= datetime('now', '-1 day');
			""";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		return await Connection.QuerySingleAsync<int>(Sql);
	}

	public async Task<(int UniqueIPs, double AvgPort)> GetNodeDiversityStatsAsync()
	{
		const string Sql = """
			SELECT 
				COUNT(DISTINCT Address) as UniqueIPs,
				AVG(CAST(Port as REAL)) as AvgPort
			FROM Nodes;
			""";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		var Result = await Connection.QuerySingleAsync<(int UniqueIPs, double AvgPort)>(Sql);
		return Result;
	}

	public async Task ForceCommitAsync()
	{
		// Force WAL checkpoint to commit data to main database file
		const string Sql = "PRAGMA wal_checkpoint(FULL);";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		await Connection.ExecuteAsync(Sql);
	}
}