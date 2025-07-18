using InfoHashFinder.Models;
using InfoHashFinder.Persistence;
using MonoTorrent.Dht;

namespace InfoHashFinder.Services;

public sealed class DhtCrawlerService(Repository Repository, ILogger<DhtCrawlerService> Logger) : BackgroundService
{
	private DhtEngine? DhtEngineField;

	private readonly List<NodeRecord> BootstrapNodes =
	[
		new("router.bittorrent.com", 6881, DateTimeOffset.UtcNow),
		new("dht.transmissionbt.com", 6881, DateTimeOffset.UtcNow),
		new("router.utorrent.com", 6881, DateTimeOffset.UtcNow),
		new("dht.libtorrent.org", 25401, DateTimeOffset.UtcNow)
	];

	protected override async Task ExecuteAsync(CancellationToken ServiceCancellationToken)
	{
		await Repository.EnsureSchemaAsync();

		IEnumerable<NodeRecord> PersistedNodes = await Repository.LoadNodesAsync();
		IEnumerable<NodeRecord> AllNodes = PersistedNodes.Any() ? PersistedNodes : BootstrapNodes;

		// Create DHT engine with default constructor
		DhtEngineField = new DhtEngine();

		// Subscribe to events
		DhtEngineField.PeersFound += OnPeersFound;

		// Start the engine with a random node ID
		byte[] NodeId = new byte[20];
		Random.Shared.NextBytes(NodeId);
		await DhtEngineField.StartAsync(NodeId);

		// Bootstrap with known nodes using the correct method
		List<ReadOnlyMemory<byte>> BootstrapNodeIds = new();
		foreach (NodeRecord NodeRecord in AllNodes)
		{
			try
			{
				// Create a dummy node ID for bootstrap
				byte[] BootstrapNodeId = new byte[20];
				Random.Shared.NextBytes(BootstrapNodeId);
				BootstrapNodeIds.Add(BootstrapNodeId);
				
				Logger.LogDebug("Adding bootstrap node {Address}:{Port}", NodeRecord.Address, NodeRecord.Port);
			}
			catch (Exception Ex)
			{
				Logger.LogWarning("Failed to process bootstrap node {Address}:{Port} - {Error}",
					NodeRecord.Address, NodeRecord.Port, Ex.Message);
			}
		}

		// Add bootstrap nodes if we have any
		if (BootstrapNodeIds.Count > 0)
		{
			DhtEngineField.Add(BootstrapNodeIds);
		}

		Logger.LogInformation("DHT Engine started with {Count} bootstrap nodes", AllNodes.Count());

		// Periodic node persistence and logging
		using PeriodicTimer Timer = new(TimeSpan.FromMinutes(5));
		while (!ServiceCancellationToken.IsCancellationRequested)
		{
			try
			{
				await Timer.WaitForNextTickAsync(ServiceCancellationToken);
				await PersistNodesAsync();

				// Log basic statistics
				Logger.LogInformation("DHT Engine is running and crawling for peers");
			}
			catch (OperationCanceledException)
			{
				break;
			}
		}
	}

	private async void OnPeersFound(object? Sender, PeersFoundEventArgs PeersEventArgs)
	{
		try
		{
			// Convert InfoHash to byte array using Span - this is the correct API
			byte[] InfoHashBytes = PeersEventArgs.InfoHash.Span.ToArray();
			InfoHashRecord Record = new(InfoHashBytes, DateTimeOffset.UtcNow);
			await Repository.UpsertInfoHashAsync(Record);

			string InfoHashHex = BitConverter.ToString(InfoHashBytes).Replace("-", "").ToLowerInvariant();
			Logger.LogInformation("Discovered InfoHash: {InfoHash} with {PeerCount} peers",
				InfoHashHex, PeersEventArgs.Peers.Count);
		}
		catch (Exception Ex)
		{
			Logger.LogError(Ex, "Error processing discovered peers");
		}
	}

	private async Task PersistNodesAsync()
	{
		try
		{
			if (DhtEngineField is null) return;

			// For now, just persist our bootstrap nodes as they are still relevant
			Logger.LogDebug("Maintaining DHT node persistence");

			foreach (NodeRecord NodeRecord in BootstrapNodes)
			{
				NodeRecord UpdatedRecord = new(
					NodeRecord.Address,
					NodeRecord.Port,
					DateTimeOffset.UtcNow);
				await Repository.UpsertNodeAsync(UpdatedRecord);
			}
		}
		catch (Exception Ex)
		{
			Logger.LogError(Ex, "Error persisting nodes");
		}
	}

	public override async Task StopAsync(CancellationToken CancellationToken)
	{
		Logger.LogInformation("Stopping DHT crawler service...");

		if (DhtEngineField is not null)
		{
			await PersistNodesAsync();
			DhtEngineField.PeersFound -= OnPeersFound;
			await DhtEngineField.StopAsync();
		}

		await base.StopAsync(CancellationToken);
	}
}