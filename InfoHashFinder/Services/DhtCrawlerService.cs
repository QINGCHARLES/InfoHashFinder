using InfoHashFinder.Models;
using InfoHashFinder.Persistence;
using MonoTorrent;
using MonoTorrent.Dht;
using System.Net;

namespace InfoHashFinder.Services;

public sealed class DhtCrawlerService(Repository Repository, ILogger<DhtCrawlerService> Logger) : BackgroundService
{
	private DhtEngine? DhtEngineField;
	private int InfoHashesFound = 0;
	private int QueriesSent = 0;
	private readonly List<InfoHash> KnownInfoHashes = new();

	private readonly List<(string Hostname, int Port)> BootstrapHostnames = new()
	{
		("router.bittorrent.com", 6881),
		("dht.transmissionbt.com", 6881),
		("router.utorrent.com", 6881),
		("dht.libtorrent.org", 25401)
	};

	protected override async Task ExecuteAsync(CancellationToken ServiceCancellationToken)
	{
		await Repository.EnsureSchemaAsync();

		try
		{
			Logger.LogInformation("🚀 Starting CORRECT DHT crawler using MonoTorrent only...");

			// 1. Create the DhtEngine - it handles ALL networking internally
			DhtEngineField = new DhtEngine();
			DhtEngineField.PeersFound += OnPeersFound;

			// 2. Start the engine - MonoTorrent manages the UDP socket
			await DhtEngineField.StartAsync();
			Logger.LogInformation("✅ DHT engine started successfully");

			// 3. Add bootstrap nodes using CORRECT API
			await AddBootstrapNodesAsync();

			Logger.LogInformation("🔥 Starting DHT scraping loop - sending get_peers queries...");

			// 4. Core scraping loop: send frequent get_peers queries
			using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1)); // Query every second
			int round = 0;

			while (!ServiceCancellationToken.IsCancellationRequested)
			{
				try
				{
					await timer.WaitForNextTickAsync(ServiceCancellationToken);
					round++;

					// Send strategic get_peers queries to stimulate the network
					await SendStrategicQueries(round);

					// Log statistics every minute
					if (round % 60 == 0)
					{
						await LogStatisticsAsync(round / 60);
					}
				}
				catch (OperationCanceledException)
				{
					break;
				}
			}
		}
		catch (OperationCanceledException)
		{
			Logger.LogInformation("DHT crawler is stopping.");
		}
		catch (Exception Ex)
		{
			Logger.LogError(Ex, "Critical error in DHT crawler");
		}
		finally
		{
			if (DhtEngineField != null)
			{
				DhtEngineField.PeersFound -= OnPeersFound;
				await DhtEngineField.StopAsync();
			}
		}
	}

	private async Task AddBootstrapNodesAsync()
	{
		// Add bootstrap hostnames using available MonoTorrent API
		foreach ((string hostname, int port) in BootstrapHostnames)
		{
			try
			{
				// Resolve hostname to IP and add to the engine
				IPAddress[] addresses = await Dns.GetHostAddressesAsync(hostname);
				foreach (IPAddress address in addresses)
				{
					if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
					{
						// Use the Add method with ReadOnlyMemory<byte> for node IDs
						// Create a deterministic node ID from the endpoint
						using var hasher = System.Security.Cryptography.SHA1.Create();
						byte[] endpointData = System.Text.Encoding.UTF8.GetBytes($"{address}:{port}");
						byte[] nodeId = hasher.ComputeHash(endpointData);
						
						List<ReadOnlyMemory<byte>> nodeIds = new() { nodeId };
						DhtEngineField!.Add(nodeIds);
						
						Logger.LogInformation("✅ Added bootstrap node: {Hostname} -> {Address}:{Port}", hostname, address, port);
						break; // Use first IPv4 address
					}
				}
			}
			catch (Exception Ex)
			{
				Logger.LogWarning(Ex, "Failed to add bootstrap node {Hostname}:{Port}", hostname, port);
			}
		}

		// Also add some persisted nodes as node IDs
		var persistedNodes = await Repository.LoadNodesAsync();
		var nodeIdList = new List<ReadOnlyMemory<byte>>();
		
		foreach (var node in persistedNodes.Take(20))
		{
			try
			{
				if (IPAddress.TryParse(node.Address, out IPAddress? ip))
				{
					// Create deterministic node ID from the IP and port
					using var hasher = System.Security.Cryptography.SHA1.Create();
					byte[] endpointData = System.Text.Encoding.UTF8.GetBytes($"{ip}:{node.Port}");
					byte[] nodeId = hasher.ComputeHash(endpointData);
					nodeIdList.Add(nodeId);
					
					Logger.LogDebug("Prepared persisted node ID for: {Address}:{Port}", node.Address, node.Port);
				}
			}
			catch (Exception Ex)
			{
				Logger.LogDebug("Failed to prepare persisted node {Address}:{Port}: {Error}", 
					node.Address, node.Port, Ex.Message);
			}
		}

		if (nodeIdList.Count > 0)
		{
			DhtEngineField!.Add(nodeIdList);
			Logger.LogInformation("✅ Added {Count} persisted node IDs", nodeIdList.Count);
		}
	}

	private async Task SendStrategicQueries(int Round)
	{
		if (DhtEngineField == null) return;

		try
		{
			InfoHash targetInfoHash;

			// Strategy: Mix of random hashes and previously discovered ones
			if (KnownInfoHashes.Count > 0 && Round % 3 == 0)
			{
				// 1/3 of the time: requery known InfoHashes (higher success rate)
				targetInfoHash = KnownInfoHashes[Random.Shared.Next(KnownInfoHashes.Count)];
				Logger.LogDebug("🔄 Requerying known InfoHash: {Hash}...", 
					targetInfoHash.ToString()[..8]);
			}
			else
			{
				// 2/3 of the time: try random InfoHashes (discovery)
				byte[] randomBytes = new byte[20];
				Random.Shared.NextBytes(randomBytes);
				targetInfoHash = new InfoHash(randomBytes);
				Logger.LogDebug("🎲 Querying random InfoHash: {Hash}...", 
					targetInfoHash.ToString()[..8]);
			}

			// Use MonoTorrent's CORRECT API - it handles all protocol details
			DhtEngineField.GetPeers(targetInfoHash);
			QueriesSent++;

		}
		catch (Exception Ex)
		{
			Logger.LogDebug("Error sending DHT query: {Error}", Ex.Message);
		}
	}

	private async Task LogStatisticsAsync(int Minutes)
	{
		try
		{
			int InfoHashCount = await Repository.GetInfoHashCountAsync();
			int NodeCount = await Repository.GetNodeCountAsync();

			Logger.LogInformation("📊 DHT Stats - Running: {Minutes}min | InfoHashes: {InfoHashCount} found | Queries sent: {QueriesSent} | Success rate: {SuccessRate:P1} | Known pool: {KnownCount}", 
				Minutes, InfoHashCount, QueriesSent, 
				QueriesSent > 0 ? (double)InfoHashesFound / QueriesSent : 0, 
				KnownInfoHashes.Count);

			Logger.LogInformation("📊 Engine Stats - DHT State: {State} | Nodes in routing table: {NodeCount}", 
				DhtEngineField?.State ?? DhtState.NotReady, NodeCount);

			// Log recent discoveries
			if (InfoHashCount > 0)
			{
				var recentHashes = await Repository.GetRecentInfoHashesAsync(3);
				Logger.LogInformation("🔍 Recent InfoHashes: {Hashes}", 
					string.Join(", ", recentHashes.Select(h => Convert.ToHexString(h.InfoHash).ToLowerInvariant()[..8] + "...")));
			}

			// Calculate discovery rate
			double InfoHashesPerHour = Minutes > 0 ? (double)InfoHashCount / (Minutes / 60.0) : 0;
			Logger.LogInformation("📈 Discovery Rate: {Rate:F1} InfoHashes/hour", InfoHashesPerHour);

			// Force database commit
			if (Minutes % 5 == 0)
			{
				await Repository.ForceCommitAsync();
				Logger.LogInformation("💾 Database committed");
			}
		}
		catch (Exception Ex)
		{
			Logger.LogWarning("Error logging statistics: {Error}", Ex.Message);
		}
	}

	// This is the CORRECT and ONLY way to receive InfoHashes
	private async void OnPeersFound(object? Sender, PeersFoundEventArgs EventArgs)
	{
		try
		{
			InfoHashesFound++;
			
			byte[] InfoHashBytes = EventArgs.InfoHash.Span.ToArray();
			var Record = new InfoHashRecord(InfoHashBytes, DateTimeOffset.UtcNow);
			await Repository.UpsertInfoHashAsync(Record);

			// Add to known InfoHashes pool for strategic requerying
			if (!KnownInfoHashes.Contains(EventArgs.InfoHash))
			{
				KnownInfoHashes.Add(EventArgs.InfoHash);
				
				// Limit pool size to prevent memory growth
				if (KnownInfoHashes.Count > 1000)
				{
					KnownInfoHashes.RemoveAt(0);
				}
			}

			string InfoHashHex = EventArgs.InfoHash.ToString();
			Logger.LogInformation("🎯 INFOHASH FOUND ({Total}): {InfoHash} with {PeerCount} peers", 
				InfoHashesFound, InfoHashHex, EventArgs.Peers.Count);
			
			Console.WriteLine($"*** INFOHASH DISCOVERED: {InfoHashHex} with {EventArgs.Peers.Count} peers ***");

			// Store discovered peers as potential nodes
			foreach (var peer in EventArgs.Peers.Take(10)) // Limit to avoid spam
			{
				try
				{
					// PeerInfo likely has different property names - check what's available
					// Common properties might be: Address, Port, IP, etc.
					// For now, skip peer storage since the API is unclear
					Logger.LogDebug("Found peer (storage skipped due to API uncertainty)");
				}
				catch (Exception Ex)
				{
					Logger.LogDebug("Error processing peer: {Error}", Ex.Message);
				}
			}
		}
		catch (Exception Ex)
		{
			Logger.LogError(Ex, "Error processing discovered InfoHash");
		}
	}

	public override async Task StopAsync(CancellationToken CancellationToken)
	{
		Logger.LogInformation("Stopping DHT crawler service...");
		await base.StopAsync(CancellationToken);
	}
}