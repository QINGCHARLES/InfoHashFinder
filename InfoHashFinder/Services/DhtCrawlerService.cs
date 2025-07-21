using InfoHashFinder.Models;
using InfoHashFinder.Persistence;
using MonoTorrent.Dht;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace InfoHashFinder.Services;

public sealed class DhtCrawlerService(Repository Repository, ILogger<DhtCrawlerService> Logger) : BackgroundService
{
	private DhtEngine? DhtEngineField;
	private UdpClient? UdpClientField;
	private int TotalPacketsReceived = 0;
	private int TotalPacketsSent = 0;
	private readonly List<IPEndPoint> DiscoveredNodes = new();

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

		// Resolve bootstrap nodes and get actual IP endpoints first
		List<IPEndPoint> BootstrapEndpoints = await ResolveBootstrapNodesAsync();
		
		if (BootstrapEndpoints.Count == 0)
		{
			Logger.LogError("No bootstrap nodes could be resolved! DHT will not function properly.");
			return;
		}

		try
		{
			// Create our own UDP client to verify network connectivity
			IPEndPoint ListenEndpoint = new(IPAddress.Any, 6881);
			UdpClientField = new UdpClient(ListenEndpoint);
			Logger.LogInformation("Created UDP client listening on {Endpoint}", ListenEndpoint);

			// Test basic UDP connectivity by sending ping packets to bootstrap nodes
			await TestUdpConnectivityAsync(BootstrapEndpoints);

			// Create DHT engine
			DhtEngineField = new DhtEngine();

			// Subscribe to PeersFound event
			DhtEngineField.PeersFound += OnPeersFound;

			// Generate a random node ID
			byte[] NodeId = new byte[20];
			Random.Shared.NextBytes(NodeId);
			
			Logger.LogInformation("Starting DHT engine with node ID: {NodeId}", 
				Convert.ToHexString(NodeId).ToLowerInvariant());
			
			// Start the engine
			await DhtEngineField.StartAsync(NodeId);
			Logger.LogInformation("DHT engine started successfully");

			// Add bootstrap nodes
			Logger.LogInformation("Adding {Count} bootstrap nodes to DHT engine...", BootstrapEndpoints.Count);
			List<ReadOnlyMemory<byte>> BootstrapNodeIds = new();
			foreach (IPEndPoint Endpoint in BootstrapEndpoints)
			{
				// Create deterministic node IDs for bootstrap nodes
				using var hasher = System.Security.Cryptography.SHA1.Create();
				byte[] EndpointData = System.Text.Encoding.UTF8.GetBytes($"{Endpoint.Address}:{Endpoint.Port}");
				byte[] BootstrapNodeId = hasher.ComputeHash(EndpointData);
				
				BootstrapNodeIds.Add(BootstrapNodeId);
				Logger.LogInformation("Added bootstrap node ID for {Endpoint}", Endpoint);
			}

			if (BootstrapNodeIds.Count > 0)
			{
				DhtEngineField.Add(BootstrapNodeIds);
				Logger.LogInformation("✅ Added {Count} bootstrap node IDs to DHT engine", BootstrapNodeIds.Count);
			}

			// Start manual DHT crawling process
			_ = Task.Run(async () => await PerformManualDhtCrawlingAsync(BootstrapEndpoints, ServiceCancellationToken));

			Logger.LogInformation("🚀 DHT engine running with enhanced InfoHash extraction...");

			// Monitor for activity and log statistics
			using PeriodicTimer Timer = new(TimeSpan.FromMinutes(1));
			int PeriodicCount = 0;
			
			while (!ServiceCancellationToken.IsCancellationRequested)
			{
				try
				{
					await Timer.WaitForNextTickAsync(ServiceCancellationToken);
					PeriodicCount++;
					
					await PersistNodesAsync();
					await LogStatisticsAsync(PeriodicCount);
				}
				catch (OperationCanceledException)
				{
					break;
				}
			}
		}
		catch (Exception Ex)
		{
			Logger.LogError(Ex, "Failed to start DHT engine");
			throw;
		}
	}

	private async Task LogStatisticsAsync(int Minutes)
	{
		try
		{
			int InfoHashCount = await Repository.GetInfoHashCountAsync();
			int NodeCount = await Repository.GetNodeCountAsync();

			Logger.LogInformation("📊 DHT Stats - Running: {Minutes}min | InfoHashes: {InfoHashCount} | Nodes: {NodeCount} | Discovered: {DiscoveredCount} | Sent: {Sent} | Received: {Received}", 
				Minutes, InfoHashCount, NodeCount, DiscoveredNodes.Count, TotalPacketsSent, TotalPacketsReceived);

			// Log recent discoveries every 5 minutes
			if (Minutes % 5 == 0 && InfoHashCount > 0)
			{
				var RecentHashes = await Repository.GetRecentInfoHashesAsync(5);
				Logger.LogInformation("🔍 Recent InfoHashes: {Hashes}", 
					string.Join(", ", RecentHashes.Select(h => Convert.ToHexString(h.InfoHash).ToLowerInvariant()[..8] + "...")));
			}

			// Force database commit every 10 minutes
			if (Minutes % 10 == 0)
			{
				await Repository.ForceCommitAsync();
				Logger.LogInformation("💾 Forced database commit to ensure data persistence");
			}
		}
		catch (Exception Ex)
		{
			Logger.LogWarning("Error logging statistics: {Error}", Ex.Message);
		}
	}

	private async Task TestUdpConnectivityAsync(List<IPEndPoint> BootstrapEndpoints)
	{
		if (UdpClientField is null) return;

		Logger.LogInformation("🔍 Testing UDP connectivity to bootstrap nodes...");

		foreach (IPEndPoint Endpoint in BootstrapEndpoints)
		{
			try
			{
				// Send a basic UDP packet to test connectivity
				byte[] TestPacket = System.Text.Encoding.UTF8.GetBytes("test");
				await UdpClientField.SendAsync(TestPacket, Endpoint);
				TotalPacketsSent++;
				Logger.LogInformation("✅ UDP test packet sent to {Endpoint}", Endpoint);
			}
			catch (Exception Ex)
			{
				Logger.LogWarning("❌ Failed to send UDP test packet to {Endpoint}: {Error}", Endpoint, Ex.Message);
			}
		}
	}

	private async Task PerformManualDhtCrawlingAsync(List<IPEndPoint> BootstrapEndpoints, CancellationToken CancellationToken)
	{
		if (UdpClientField is null)
		{
			Logger.LogError("UDP client is null, cannot perform manual DHT crawling");
			return;
		}

		try
		{
			await Task.Delay(TimeSpan.FromSeconds(10), CancellationToken);

			Logger.LogInformation("🔄 Starting enhanced DHT crawling with InfoHash extraction...");

			// Start listening for incoming UDP packets
			_ = Task.Run(async () => await ListenForIncomingDhtMessagesAsync(CancellationToken));

			// Start aggressive node discovery
			_ = Task.Run(async () => await PerformAggressiveNodeDiscoveryAsync(BootstrapEndpoints, CancellationToken));

			// Send DHT messages to bootstrap nodes periodically
			using PeriodicTimer CrawlTimer = new(TimeSpan.FromMinutes(1));
			int CrawlCount = 0;

			while (!CancellationToken.IsCancellationRequested && CrawlCount < 60)
			{
				try
				{
					await CrawlTimer.WaitForNextTickAsync(CancellationToken);
					CrawlCount++;

					// Query bootstrap nodes
					await SendDhtQueriesAsync(BootstrapEndpoints, CrawlCount);

					// Query discovered nodes
					await QueryDiscoveredNodesAsync(CrawlCount);
				}
				catch (OperationCanceledException)
				{
					break;
				}
			}

			Logger.LogInformation("✅ Manual DHT crawling completed {Count} rounds", CrawlCount);
		}
		catch (Exception Ex)
		{
			Logger.LogError(Ex, "Error during manual DHT crawling");
		}
	}

	private async Task PerformAggressiveNodeDiscoveryAsync(List<IPEndPoint> BootstrapEndpoints, CancellationToken CancellationToken)
	{
		// Wait for initial bootstrap
		await Task.Delay(TimeSpan.FromSeconds(30), CancellationToken);

		Logger.LogInformation("🔍 Starting aggressive node discovery to find active DHT participants...");

		using PeriodicTimer DiscoveryTimer = new(TimeSpan.FromSeconds(30));
		int DiscoveryRound = 0;

		while (!CancellationToken.IsCancellationRequested && DiscoveryRound < 120)
		{
			try
			{
				await DiscoveryTimer.WaitForNextTickAsync(CancellationToken);
				DiscoveryRound++;

				// Send find_node queries with well-distributed target IDs
				foreach (IPEndPoint Endpoint in BootstrapEndpoints.Concat(DiscoveredNodes.Take(10)))
				{
					for (int i = 0; i < 5; i++)
					{
						await SendAggressiveFindNodeAsync(Endpoint);
						await Task.Delay(100, CancellationToken);
					}
				}

				Logger.LogDebug("🔍 Aggressive discovery round {Round} completed", DiscoveryRound);
			}
			catch (OperationCanceledException)
			{
				break;
			}
		}
	}

	private async Task SendAggressiveFindNodeAsync(IPEndPoint Endpoint)
	{
		if (UdpClientField is null) return;

		try
		{
			byte[] TransactionId = new byte[2];
			Random.Shared.NextBytes(TransactionId);
			
			byte[] NodeId = new byte[20];
			Random.Shared.NextBytes(NodeId);

			// Create targets spread across the DHT keyspace
			byte[] TargetId = new byte[20];
			Random.Shared.NextBytes(TargetId);

			// Create find_node query
			string FindNodeMessage = $"d1:ad2:id20:{Encoding.Latin1.GetString(NodeId)}6:target20:{Encoding.Latin1.GetString(TargetId)}e1:q9:find_node1:t2:{Encoding.Latin1.GetString(TransactionId)}1:y1:qe";
			byte[] FindNodeBytes = Encoding.Latin1.GetBytes(FindNodeMessage);

			await UdpClientField.SendAsync(FindNodeBytes, Endpoint);
			TotalPacketsSent++;
		}
		catch (Exception Ex)
		{
			Logger.LogDebug("Failed to send aggressive find_node to {Endpoint}: {Error}", Endpoint, Ex.Message);
		}
	}

	private async Task QueryDiscoveredNodesAsync(int Round)
	{
		if (DiscoveredNodes.Count == 0) return;

		// Query some of the discovered nodes for InfoHashes
		var NodesToQuery = DiscoveredNodes.Take(20).ToList();
		
		foreach (IPEndPoint Node in NodesToQuery)
		{
			try
			{
				// Send get_peers queries for random InfoHashes
				for (int i = 0; i < 3; i++)
				{
					await SendGetPeersQueryAsync(Node);
					await Task.Delay(100);
				}
			}
			catch (Exception Ex)
			{
				Logger.LogDebug("Failed to query discovered node {Node}: {Error}", Node, Ex.Message);
			}
		}

		Logger.LogDebug("📡 Queried {Count} discovered nodes in round {Round}", NodesToQuery.Count, Round);
	}

	private async Task SendDhtQueriesAsync(List<IPEndPoint> BootstrapEndpoints, int Round)
	{
		if (UdpClientField is null) return;

		Logger.LogInformation("📡 Sending DHT queries - Round {Round}", Round);

		foreach (IPEndPoint Endpoint in BootstrapEndpoints)
		{
			try
			{
				// Send ping first
				await SendDhtPingAsync(Endpoint);
				await Task.Delay(200);

				// Send find_node query to discover more nodes
				await SendFindNodeQueryAsync(Endpoint);
				await Task.Delay(200);

				// Send get_peers query for random infohashes to trigger responses
				await SendGetPeersQueryAsync(Endpoint);
				await Task.Delay(200);
			}
			catch (Exception Ex)
			{
				Logger.LogWarning("Failed to send DHT queries to {Endpoint}: {Error}", Endpoint, Ex.Message);
			}
		}
	}

	private async Task SendDhtPingAsync(IPEndPoint Endpoint)
	{
		if (UdpClientField is null) return;

		byte[] TransactionId = new byte[2];
		Random.Shared.NextBytes(TransactionId);
		
		byte[] NodeId = new byte[20];
		Random.Shared.NextBytes(NodeId);

		// Create proper bencode ping message
		string PingMessage = $"d1:ad2:id20:{Encoding.Latin1.GetString(NodeId)}e1:q4:ping1:t2:{Encoding.Latin1.GetString(TransactionId)}1:y1:qe";
		byte[] PingBytes = Encoding.Latin1.GetBytes(PingMessage);

		await UdpClientField.SendAsync(PingBytes, Endpoint);
		TotalPacketsSent++;
		Logger.LogDebug("📤 Sent DHT ping to {Endpoint} (Packet #{PacketNum})", Endpoint, TotalPacketsSent);
	}

	private async Task SendFindNodeQueryAsync(IPEndPoint Endpoint)
	{
		if (UdpClientField is null) return;

		byte[] TransactionId = new byte[2];
		Random.Shared.NextBytes(TransactionId);
		
		byte[] NodeId = new byte[20];
		Random.Shared.NextBytes(NodeId);

		byte[] TargetId = new byte[20];
		Random.Shared.NextBytes(TargetId);

		// Create find_node query
		string FindNodeMessage = $"d1:ad2:id20:{Encoding.Latin1.GetString(NodeId)}6:target20:{Encoding.Latin1.GetString(TargetId)}e1:q9:find_node1:t2:{Encoding.Latin1.GetString(TransactionId)}1:y1:qe";
		byte[] FindNodeBytes = Encoding.Latin1.GetBytes(FindNodeMessage);

		await UdpClientField.SendAsync(FindNodeBytes, Endpoint);
		TotalPacketsSent++;
		Logger.LogDebug("📤 Sent find_node query to {Endpoint} (Packet #{PacketNum})", Endpoint, TotalPacketsSent);
	}

	private async Task SendGetPeersQueryAsync(IPEndPoint Endpoint)
	{
		if (UdpClientField is null) return;

		byte[] TransactionId = new byte[2];
		Random.Shared.NextBytes(TransactionId);
		
		byte[] NodeId = new byte[20];
		Random.Shared.NextBytes(NodeId);

		byte[] InfoHash = new byte[20];
		Random.Shared.NextBytes(InfoHash);

		// Create get_peers query - this should trigger nodes to respond with info about torrents
		string GetPeersMessage = $"d1:ad2:id20:{Encoding.Latin1.GetString(NodeId)}9:info_hash20:{Encoding.Latin1.GetString(InfoHash)}e1:q9:get_peers1:t2:{Encoding.Latin1.GetString(TransactionId)}1:y1:qe";
		byte[] GetPeersBytes = Encoding.Latin1.GetBytes(GetPeersMessage);

		await UdpClientField.SendAsync(GetPeersBytes, Endpoint);
		TotalPacketsSent++;
		Logger.LogDebug("📤 Sent get_peers query to {Endpoint} (Packet #{PacketNum})", Endpoint, TotalPacketsSent);
	}

	private async Task ListenForIncomingDhtMessagesAsync(CancellationToken CancellationToken)
	{
		if (UdpClientField is null) return;

		Logger.LogInformation("👂 Started listening for incoming DHT messages...");

		try
		{
			while (!CancellationToken.IsCancellationRequested)
			{
				try
				{
					var Result = await UdpClientField.ReceiveAsync().WaitAsync(CancellationToken);
					TotalPacketsReceived++;
					
					Logger.LogDebug("📥 Received UDP packet #{PacketNum} from {RemoteEndpoint}: {Length} bytes", 
						TotalPacketsReceived, Result.RemoteEndPoint, Result.Buffer.Length);

					// Process the DHT message
					await ProcessIncomingDhtMessageAsync(Result.Buffer, Result.RemoteEndPoint);
				}
				catch (OperationCanceledException)
				{
					break;
				}
				catch (Exception Ex)
				{
					Logger.LogWarning("Error receiving UDP packet: {Error}", Ex.Message);
				}
			}
		}
		catch (Exception Ex)
		{
			Logger.LogError(Ex, "Error in DHT message listener");
		}
	}

	private async Task ProcessIncomingDhtMessageAsync(byte[] MessageData, IPEndPoint RemoteEndpoint)
	{
		try
		{
			string Message = Encoding.Latin1.GetString(MessageData);
			
			// Parse compact node info to discover new nodes
			await ExtractAndStoreNodesFromMessage(Message, RemoteEndpoint);

			// Look for actual DHT queries that contain InfoHashes
			await ProcessDhtQueriesForInfoHashes(Message, RemoteEndpoint);

			// Extract InfoHashes from get_peers queries and announce_peer messages
			await ExtractInfoHashesFromDhtMessage(Message, RemoteEndpoint);

			// Extract and store new node addresses for future crawling
			await ExtractNodesFromDhtMessage(Message, RemoteEndpoint);
		}
		catch (Exception Ex)
		{
			Logger.LogDebug("Error processing DHT message from {RemoteEndpoint}: {Error}", RemoteEndpoint, Ex.Message);
		}
	}

	private async Task ExtractAndStoreNodesFromMessage(string Message, IPEndPoint RemoteEndpoint)
	{
		try
		{
			// Parse compact node format from nodes responses
			int NodesIndex = Message.IndexOf("5:nodes");
			if (NodesIndex >= 0)
			{
				// Add this responding node to our discovered list
				if (!DiscoveredNodes.Any(n => n.Address.Equals(RemoteEndpoint.Address) && n.Port == RemoteEndpoint.Port))
				{
					DiscoveredNodes.Add(RemoteEndpoint);
					Logger.LogInformation("📍 Added new discovered node: {Endpoint} (Total: {Count})", 
						RemoteEndpoint, DiscoveredNodes.Count);
				}

				// Try to parse the compact node format (each node is 26 bytes: 20 byte ID + 4 byte IP + 2 byte port)
				// This is a simplified parser - real implementation would need proper bencode parsing
				Logger.LogDebug("📍 Parsing nodes list from {RemoteEndpoint}", RemoteEndpoint);
			}
		}
		catch (Exception Ex)
		{
			Logger.LogDebug("Error extracting nodes: {Error}", Ex.Message);
		}
	}

	private async Task ProcessDhtQueriesForInfoHashes(string Message, IPEndPoint RemoteEndpoint)
	{
		try
		{
			// Look for incoming DHT queries that might contain InfoHashes
			if (Message.Contains("1:q") && (Message.Contains("get_peers") || Message.Contains("announce_peer")))
			{
				Logger.LogInformation("🎯 Received incoming DHT query from {RemoteEndpoint}: {MessageType}", 
					RemoteEndpoint, Message.Contains("get_peers") ? "get_peers" : "announce_peer");
				
				// This is where real InfoHashes would be found
				await ExtractInfoHashesFromDhtMessage(Message, RemoteEndpoint);
			}
		}
		catch (Exception Ex)
		{
			Logger.LogDebug("Error processing DHT queries: {Error}", Ex.Message);
		}
	}

	private async Task ExtractInfoHashesFromDhtMessage(string Message, IPEndPoint RemoteEndpoint)
	{
		try
		{
			// Look for info_hash fields in DHT messages
			int InfoHashIndex = Message.IndexOf("9:info_hash20:");
			if (InfoHashIndex >= 0)
			{
				int HashStart = InfoHashIndex + "9:info_hash20:".Length;
				if (HashStart + 20 <= Message.Length)
				{
					byte[] InfoHashBytes = Encoding.Latin1.GetBytes(Message.Substring(HashStart, 20));
					
					// Store the discovered InfoHash
					InfoHashRecord Record = new(InfoHashBytes, DateTimeOffset.UtcNow);
					await Repository.UpsertInfoHashAsync(Record);

					string InfoHashHex = Convert.ToHexString(InfoHashBytes).ToLowerInvariant();
					Logger.LogInformation("🎯 EXTRACTED InfoHash: {InfoHash} from {RemoteEndpoint}", 
						InfoHashHex, RemoteEndpoint);
					
					// Also log to console for immediate visibility
					Console.WriteLine($"*** INFOHASH FOUND: {InfoHashHex} ***");
				}
			}

			// Look for announce_peer messages
			if (Message.Contains("announce_peer"))
			{
				Logger.LogInformation("🎯 ANNOUNCE_PEER message from {RemoteEndpoint}", RemoteEndpoint);
				int HashIndex2 = Message.IndexOf("9:info_hash20:");
				if (HashIndex2 >= 0)
				{
					int HashStart2 = HashIndex2 + "9:info_hash20:".Length;
					if (HashStart2 + 20 <= Message.Length)
					{
						byte[] InfoHashBytes = Encoding.Latin1.GetBytes(Message.Substring(HashStart2, 20));
						
						InfoHashRecord Record = new(InfoHashBytes, DateTimeOffset.UtcNow);
						await Repository.UpsertInfoHashAsync(Record);

						string InfoHashHex = Convert.ToHexString(InfoHashBytes).ToLowerInvariant();
						Logger.LogInformation("🎯 CAPTURED InfoHash from announce_peer: {InfoHash} from {RemoteEndpoint}", 
							InfoHashHex, RemoteEndpoint);
						
						Console.WriteLine($"*** ANNOUNCE INFOHASH: {InfoHashHex} ***");
					}
				}
			}
		}
		catch (Exception Ex)
		{
			Logger.LogDebug("Error extracting InfoHashes: {Error}", Ex.Message);
		}
	}

	private async Task ExtractNodesFromDhtMessage(string Message, IPEndPoint RemoteEndpoint)
	{
		try
		{
			// Add any node that responds to us
			if (Message.Contains("1:y1:r"))  // Response message
			{
				NodeRecord ResponderNode = new(RemoteEndpoint.Address.ToString(), RemoteEndpoint.Port, DateTimeOffset.UtcNow);
				await Repository.UpsertNodeAsync(ResponderNode);
				Logger.LogDebug("📍 Added responding node {RemoteEndpoint} to database", RemoteEndpoint);
			}
		}
		catch (Exception Ex)
		{
			Logger.LogDebug("Error extracting nodes: {Error}", Ex.Message);
		}
	}

	private async Task<List<IPEndPoint>> ResolveBootstrapNodesAsync()
	{
		List<IPEndPoint> ResolvedEndpoints = new();

		// First, try to load persisted IP addresses from database
		IEnumerable<NodeRecord> PersistedNodes = await Repository.LoadNodesAsync();
		foreach (NodeRecord Node in PersistedNodes)
		{
			try
			{
				// Check if the address is already an IP address
				if (IPAddress.TryParse(Node.Address, out IPAddress? ParsedIp))
				{
					ResolvedEndpoints.Add(new IPEndPoint(ParsedIp, Node.Port));
					Logger.LogDebug("Using persisted IP {Address}:{Port}", Node.Address, Node.Port);
				}
			}
			catch (Exception Ex)
			{
				Logger.LogWarning("Failed to use persisted node {Address}:{Port} - {Error}",
					Node.Address, Node.Port, Ex.Message);
			}
		}

		// If we have good persisted IPs, use them; otherwise resolve hostnames
		if (ResolvedEndpoints.Count > 0)
		{
			Logger.LogInformation("Using {Count} persisted IP addresses for bootstrap", ResolvedEndpoints.Count);
			return ResolvedEndpoints;
		}

		// Resolve hostnames to IP addresses
		Logger.LogInformation("No persisted IPs found, resolving bootstrap hostnames...");
		foreach ((string Hostname, int Port) in BootstrapHostnames)
		{
			try
			{
				Logger.LogInformation("Resolving hostname {Hostname}...", Hostname);
				IPAddress[] Addresses = await Dns.GetHostAddressesAsync(Hostname);
				
				foreach (IPAddress Address in Addresses)
				{
					// Prefer IPv4 for better DHT compatibility
					if (Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
					{
						IPEndPoint Endpoint = new(Address, Port);
						ResolvedEndpoints.Add(Endpoint);
						
						// Persist the resolved IP address for future use
						NodeRecord IpRecord = new(Address.ToString(), Port, DateTimeOffset.UtcNow);
						await Repository.UpsertNodeAsync(IpRecord);
						
						Logger.LogInformation("✅ Resolved {Hostname} -> {IpAddress}:{Port}", 
							Hostname, Address, Port);
						break; // Use first IPv4 address for each hostname
					}
				}
			}
			catch (Exception Ex)
			{
				Logger.LogError("❌ Failed to resolve hostname {Hostname} - {Error}", Hostname, Ex.Message);
			}
		}

		Logger.LogInformation("Successfully resolved {Count} bootstrap endpoints from hostnames", ResolvedEndpoints.Count);
		return ResolvedEndpoints;
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
			Logger.LogInformation("🎯 DISCOVERED InfoHash via MonoTorrent: {InfoHash} with {PeerCount} peers",
				InfoHashHex, PeersEventArgs.Peers.Count);
			
			Console.WriteLine($"*** MONOTORRENT INFOHASH: {InfoHashHex} ***");
			
			// This is great news - the DHT is working!
			Logger.LogInformation("✅ DHT is successfully discovering peers and infohashes!");
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

			Logger.LogDebug("Persisting DHT node data...");

			// Update the timestamp on our known good bootstrap nodes
			IEnumerable<NodeRecord> ExistingNodes = await Repository.LoadNodesAsync();
			foreach (NodeRecord Node in ExistingNodes)
			{
				// Only update IP addresses (not hostnames)
				if (IPAddress.TryParse(Node.Address, out _))
				{
					NodeRecord UpdatedRecord = new(Node.Address, Node.Port, DateTimeOffset.UtcNow);
					await Repository.UpsertNodeAsync(UpdatedRecord);
				}
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

		UdpClientField?.Dispose();

		await base.StopAsync(CancellationToken);
	}
}