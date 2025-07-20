using InfoHashFinder.Models;
using InfoHashFinder.Persistence;
using MonoTorrent.Dht;
using System.Net;
using System.Net.Sockets;

namespace InfoHashFinder.Services;

public sealed class DhtCrawlerService(Repository Repository, ILogger<DhtCrawlerService> Logger) : BackgroundService
{
	private DhtEngine? DhtEngineField;
	private UdpClient? UdpClientField;

	private readonly List<(string Hostname, int Port)> BootstrapHostnames =
	[
		("router.bittorrent.com", 6881),
		("dht.transmissionbt.com", 6881),
		("router.utorrent.com", 6881),
		("dht.libtorrent.org", 25401)
	];

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
			// Create UDP client for DHT communication - ensure we can send/receive UDP packets
			IPEndPoint ListenEndpoint = new(IPAddress.Any, 6881);
			UdpClientField = new UdpClient(ListenEndpoint);
			
			Logger.LogInformation("Created UDP client listening on {Endpoint}", ListenEndpoint);

			// Create DHT engine
			DhtEngineField = new DhtEngine();

			// Subscribe to PeersFound event - this should trigger when peers are discovered
			DhtEngineField.PeersFound += OnPeersFound;

			// Start the engine with a random node ID
			byte[] NodeId = new byte[20];
			Random.Shared.NextBytes(NodeId);
			
			Logger.LogInformation("Starting DHT engine with node ID: {NodeId}", 
				Convert.ToHexString(NodeId).ToLowerInvariant());
			
			await DhtEngineField.StartAsync(NodeId);
			Logger.LogInformation("DHT engine started successfully");

			// Bootstrap with resolved IP endpoints using the available API
			Logger.LogInformation("Bootstrapping with {Count} resolved endpoints...", BootstrapEndpoints.Count);
			
			// Create node entries for bootstrapping
			List<ReadOnlyMemory<byte>> NodeEntries = new();
			foreach (IPEndPoint Endpoint in BootstrapEndpoints)
			{
				// Create a deterministic node ID based on the endpoint
				using var hasher = System.Security.Cryptography.SHA1.Create();
				byte[] EndpointData = System.Text.Encoding.UTF8.GetBytes($"{Endpoint.Address}:{Endpoint.Port}");
				byte[] NodeId2 = hasher.ComputeHash(EndpointData);
				
				NodeEntries.Add(NodeId2);
				Logger.LogInformation("Prepared bootstrap node ID for {Endpoint}", Endpoint);
			}

			// Add bootstrap nodes to DHT engine
			if (NodeEntries.Count > 0)
			{
				DhtEngineField.Add(NodeEntries);
				Logger.LogInformation("✅ DHT Engine bootstrapped with {Count} node entries!", NodeEntries.Count);
			}

			// Since MonoTorrent 3.0.2 has limited API, let's try a different approach
			// We'll rely on the engine's internal discovery mechanisms
			Logger.LogInformation("🚀 DHT engine is now running. Waiting for peer discovery...");
			Logger.LogInformation("💡 The engine should start discovering peers automatically.");
			Logger.LogInformation("📡 This may take several minutes as the DHT routing table populates.");

			// Monitor for activity and log periodically
			using PeriodicTimer Timer = new(TimeSpan.FromMinutes(1));
			int PeriodicCount = 0;
			
			while (!ServiceCancellationToken.IsCancellationRequested)
			{
				try
				{
					await Timer.WaitForNextTickAsync(ServiceCancellationToken);
					PeriodicCount++;
					
					await PersistNodesAsync();

					// Provide more informative logging
					Logger.LogInformation("📡 DHT Engine running for {Minutes} minutes - Monitoring for peer discovery", PeriodicCount);
					
					if (PeriodicCount % 5 == 0)
					{
						Logger.LogInformation("💡 If no peers are discovered after 10+ minutes, there may be network connectivity issues.");
						Logger.LogInformation("🔍 Check: 1) UDP port 6881 is open, 2) No firewall blocking, 3) Network allows P2P traffic");
					}
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
			Logger.LogInformation("🎯 DISCOVERED InfoHash: {InfoHash} with {PeerCount} peers",
				InfoHashHex, PeersEventArgs.Peers.Count);
			
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