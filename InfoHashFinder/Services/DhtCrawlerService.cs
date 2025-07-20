using InfoHashFinder.Models;
using InfoHashFinder.Persistence;
using MonoTorrent.Dht;
using System.Net;

namespace InfoHashFinder.Services;

public sealed class DhtCrawlerService(Repository Repository, ILogger<DhtCrawlerService> Logger) : BackgroundService
{
	private DhtEngine? DhtEngineField;

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

		// Create DHT engine with default constructor
		DhtEngineField = new DhtEngine();

		// Subscribe to events
		DhtEngineField.PeersFound += OnPeersFound;

		// Start the engine with a random node ID
		byte[] NodeId = new byte[20];
		Random.Shared.NextBytes(NodeId);
		await DhtEngineField.StartAsync(NodeId);

		// Now we have resolved IP endpoints, but we need to work within MonoTorrent's API
		// Create deterministic node IDs based on the actual IP endpoints
		List<ReadOnlyMemory<byte>> NodeIds = new();
		foreach (IPEndPoint Endpoint in BootstrapEndpoints)
		{
			// Create a deterministic node ID based on the endpoint IP and port
			byte[] EndpointBytes = Endpoint.Address.GetAddressBytes();
			byte[] PortBytes = BitConverter.GetBytes((ushort)Endpoint.Port);
			
			byte[] BootstrapNodeId = new byte[20];
			
			// Copy IP address bytes (4 bytes for IPv4)
			Array.Copy(EndpointBytes, 0, BootstrapNodeId, 0, Math.Min(EndpointBytes.Length, 16));
			
			// Copy port bytes
			Array.Copy(PortBytes, 0, BootstrapNodeId, 16, 2);
			
			// Fill remaining bytes with a deterministic pattern based on IP+port
			uint Hash = (uint)Endpoint.GetHashCode();
			BootstrapNodeId[18] = (byte)(Hash & 0xFF);
			BootstrapNodeId[19] = (byte)((Hash >> 8) & 0xFF);
			
			NodeIds.Add(BootstrapNodeId);
			Logger.LogInformation("Prepared bootstrap node ID for {Address}:{Port}", 
				Endpoint.Address, Endpoint.Port);
		}

		// Add all bootstrap node IDs to the DHT engine
		if (NodeIds.Count > 0)
		{
			DhtEngineField.Add(NodeIds);
			Logger.LogInformation("DHT Engine bootstrapped with {Count} node IDs from resolved IP endpoints", NodeIds.Count);
		}
		else
		{
			Logger.LogWarning("No bootstrap nodes were successfully prepared!");
		}

		Logger.LogInformation("DHT Engine started and bootstrapped with {Count} resolved endpoints", BootstrapEndpoints.Count);
		Logger.LogInformation("DHT should now start discovering peers from the network...");

		// Periodic node persistence and logging
		using PeriodicTimer Timer = new(TimeSpan.FromMinutes(5));
		while (!ServiceCancellationToken.IsCancellationRequested)
		{
			try
			{
				await Timer.WaitForNextTickAsync(ServiceCancellationToken);
				await PersistNodesAsync();

				// Log DHT statistics with resolved endpoints info
				Logger.LogInformation("DHT Engine active - Bootstrapped from {Count} resolved IPs: {IPs}", 
					BootstrapEndpoints.Count,
					string.Join(", ", BootstrapEndpoints.Select(ep => $"{ep.Address}:{ep.Port}")));
			}
			catch (OperationCanceledException)
			{
				break;
			}
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
			Logger.LogInformation("🎯 FOUND InfoHash: {InfoHash} with {PeerCount} peers",
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

		await base.StopAsync(CancellationToken);
	}
}