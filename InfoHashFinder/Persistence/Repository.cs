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

	public async Task ForceCommitAsync()
	{
		// Force WAL checkpoint to commit data to main database file
		const string Sql = "PRAGMA wal_checkpoint(FULL);";
		await using SqliteConnection Connection = await CreateConnectionAsync();
		await Connection.ExecuteAsync(Sql);
	}
}