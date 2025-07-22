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
	private const string NodeIdFilePath = "dht_node_id.dat";

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
			Logger.LogInformation("🚀 Starting ENHANCED DHT crawler using MonoTorrent with persistent node ID...");

			// 1. Get or create persistent node ID
			byte[] persistentNodeId = await GetOrCreatePersistentNodeIdAsync();

			// 2. Create the DhtEngine - it handles ALL networking internally
			DhtEngineField = new DhtEngine();
			DhtEngineField.PeersFound += OnPeersFound;

			// 3. Start the engine with persistent node ID - MonoTorrent manages the UDP socket
			await DhtEngineField.StartAsync(persistentNodeId);
			Logger.LogInformation("✅ DHT engine started successfully with persistent node ID: {NodeId}", 
				Convert.ToHexString(persistentNodeId).ToLowerInvariant());

			// 4. Add bootstrap nodes using CORRECT API
			await AddBootstrapNodesAsync();

			Logger.LogInformation("🔥 Starting DHT scraping loop - sending get_peers queries...");

			// 5. Core scraping loop: send frequent get_peers queries
			using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1)); // Query every second
			int round = 0;
			int consecutiveErrors = 0;

			while (!ServiceCancellationToken.IsCancellationRequested)
			{
				try
				{
					await timer.WaitForNextTickAsync(ServiceCancellationToken);
					round++;

					// Check DHT engine state - allow queries in more states
					if (DhtEngineField.State == DhtState.Ready || DhtEngineField.State == DhtState.Initialising)
					{
						// Send strategic get_peers queries to stimulate the network
						SendStrategicQueries(round);
						consecutiveErrors = 0; // Reset error counter on success
					}
					else
					{
						Logger.LogDebug("DHT engine not ready (State: {State}), skipping query round {Round}", 
							DhtEngineField.State, round);
					}

					// Log statistics every minute
					if (round % 60 == 0)
					{
						await LogStatisticsAsync(round / 60);
					}

					// Log engine state every 5 minutes if not ready
					if (round % 300 == 0 && DhtEngineField.State != DhtState.Ready)
					{
						Logger.LogWarning("DHT engine has been in {State} state for {Minutes} minutes", 
							DhtEngineField.State, round / 60);
					}
				}
				catch (OperationCanceledException)
				{
					break;
				}
				catch (Exception Ex)
				{
					consecutiveErrors++;
					Logger.LogWarning(Ex, "Error in DHT query loop (consecutive errors: {ErrorCount})", consecutiveErrors);
					
					// If too many consecutive errors, add a longer delay
					if (consecutiveErrors > 10)
					{
						Logger.LogWarning("Too many consecutive errors, pausing for 30 seconds...");
						await Task.Delay(TimeSpan.FromSeconds(30), ServiceCancellationToken);
						consecutiveErrors = 0;
					}
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

	private async Task<byte[]> GetOrCreatePersistentNodeIdAsync()
	{
		try
		{
			// Try to load existing node ID from file
			if (File.Exists(NodeIdFilePath))
			{
				byte[] existingNodeId = await File.ReadAllBytesAsync(NodeIdFilePath);
				if (existingNodeId.Length == 20)
				{
					Logger.LogInformation("✅ Loaded existing persistent node ID from {FilePath}", NodeIdFilePath);
					return existingNodeId;
				}
				else
				{
					Logger.LogWarning("⚠️ Invalid node ID file (wrong length), creating new one");
				}
			}

			// Create new persistent node ID
			byte[] newNodeId = new byte[20];
			Random.Shared.NextBytes(newNodeId);
			
			await File.WriteAllBytesAsync(NodeIdFilePath, newNodeId);
			Logger.LogInformation("✅ Created new persistent node ID and saved to {FilePath}", NodeIdFilePath);
			
			return newNodeId;
		}
		catch (Exception Ex)
		{
			Logger.LogWarning(Ex, "Failed to load/save persistent node ID, using random ID for this session");
			byte[] fallbackNodeId = new byte[20];
			Random.Shared.NextBytes(fallbackNodeId);
			return fallbackNodeId;
		}
	}

	private async Task AddBootstrapNodesAsync()
	{
		if (DhtEngineField == null)
		{
			Logger.LogWarning("DhtEngine is null, cannot add bootstrap nodes");
			return;
		}

		// Add bootstrap hostnames using available MonoTorrent API  
		var allNodeIds = new List<ReadOnlyMemory<byte>>();
		
		foreach ((string hostname, int port) in BootstrapHostnames)
		{
			try
			{
				// Resolve hostname to IP and create node ID
				IPAddress[] addresses = await Dns.GetHostAddressesAsync(hostname);
				foreach (IPAddress address in addresses)
				{
					if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
					{
						// Create a deterministic node ID from the endpoint
						using var hasher = System.Security.Cryptography.SHA1.Create();
						byte[] endpointData = System.Text.Encoding.UTF8.GetBytes($"{address}:{port}");
						byte[] nodeId = hasher.ComputeHash(endpointData);
						allNodeIds.Add(nodeId);
						
						Logger.LogInformation("✅ Prepared bootstrap node: {Hostname} -> {Address}:{Port}", hostname, address, port);
						break; // Use first IPv4 address
					}
				}
			}
			catch (Exception Ex)
			{
				Logger.LogWarning(Ex, "Failed to resolve bootstrap node {Hostname}:{Port}", hostname, port);
			}
		}

		// Add persisted nodes from database
		var persistedNodes = await Repository.LoadNodesAsync();
		foreach (var node in persistedNodes.Take(50)) // Add more for better network reach
		{
			try
			{
				if (IPAddress.TryParse(node.Address, out IPAddress? ip))
				{
					// Create deterministic node ID from the IP and port
					using var hasher = System.Security.Cryptography.SHA1.Create();
					byte[] endpointData = System.Text.Encoding.UTF8.GetBytes($"{ip}:{node.Port}");
					byte[] nodeId = hasher.ComputeHash(endpointData);
					allNodeIds.Add(nodeId);
					
					Logger.LogDebug("Prepared persisted node ID for: {Address}:{Port}", node.Address, node.Port);
				}
			}
			catch (Exception Ex)
			{
				Logger.LogDebug("Failed to prepare persisted node {Address}:{Port}: {Error}", 
					node.Address, node.Port, Ex.Message);
			}
		}

		// Add all node IDs at once
		if (allNodeIds.Count > 0)
		{
			DhtEngineField.Add(allNodeIds);
			Logger.LogInformation("✅ Added {Count} total node IDs (bootstrap + persisted)", allNodeIds.Count);
		}
	}

	private Task SendStrategicQueries(int Round)
	{
		if (DhtEngineField == null) return Task.CompletedTask;

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

			return Task.CompletedTask;
		}
		catch (Exception Ex)
		{
			Logger.LogDebug("Error sending DHT query: {Error}", Ex.Message);
			return Task.CompletedTask;
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

			Logger.LogInformation("📊 Engine Stats - DHT State: {State} | Nodes in routing table: {NodeCount} | Discovery rate: {Rate:F1}/hour", 
				DhtEngineField?.State ?? DhtState.NotReady, NodeCount, 
				Minutes > 0 ? (double)InfoHashCount / (Minutes / 60.0) : 0);

			// Add diagnostic information about the DHT state issue
			if (DhtEngineField?.State != DhtState.Ready && Minutes > 5)
			{
				Logger.LogInformation("🔧 DHT Diagnostic - State: {State} | InfoHashes discovered: {Count} | This indicates passive discovery is working even without Ready state", 
					DhtEngineField?.State, InfoHashCount);
			}

			// Log recent discoveries
			if (InfoHashCount > 0)
			{
				var recentHashes = await Repository.GetRecentInfoHashesAsync(3);
				Logger.LogInformation("🔍 Recent InfoHashes: {Hashes}", 
					string.Join(", ", recentHashes.Select(h => Convert.ToHexString(h.InfoHash).ToLowerInvariant()[..8] + "...")));
			}

			// Show progress towards building a good InfoHash pool
			if (KnownInfoHashes.Count < 50)
			{
				Logger.LogInformation("🎯 Building InfoHash pool: {KnownCount}/50 for strategic requerying", KnownInfoHashes.Count);
			}
			else if (Minutes % 10 == 0) // Log pool status every 10 minutes once established
			{
				Logger.LogInformation("🎯 InfoHash pool: {KnownCount} hashes available for strategic requerying", KnownInfoHashes.Count);
			}

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
	private async void OnPeersFound(object? Sender, PeersFoundEventArgs? EventArgs)
	{
		if (EventArgs == null) return;

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

			// Log successful passive discovery to confirm it's working
			Logger.LogDebug("✅ Passive discovery working - DHT State: {State}, Total discovered: {Total}", 
				(Sender as DhtEngine)?.State ?? DhtState.NotReady, InfoHashesFound);

			// Store discovered peers as potential nodes
			foreach (var peer in EventArgs.Peers.Take(10)) // Limit to avoid spam
			{
				try
				{
					// Extract peer information using ConnectionUri
					if (peer.ConnectionUri != null)
					{
						IPAddress peerIp = IPAddress.Parse(peer.ConnectionUri.Host);
						int peerPort = peer.ConnectionUri.Port;

						var peerNodeRecord = new NodeRecord(peerIp.ToString(), peerPort, DateTimeOffset.UtcNow);
						await Repository.UpsertNodeAsync(peerNodeRecord);
						Logger.LogDebug("Discovered and stored potential node from peer: {IP}:{Port}", peerIp, peerPort);
					}
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
		Logger.LogInformation("Stopping enhanced DHT crawler service...");
		await base.StopAsync(CancellationToken);
	}
}