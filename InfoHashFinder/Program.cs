using InfoHashFinder.Persistence;
using InfoHashFinder.Services;

HostApplicationBuilder Builder = Host.CreateApplicationBuilder(args);

// Register Repository as singleton
Builder.Services.AddSingleton<Repository>();

// Register the DHT crawler service
Builder.Services.AddHostedService<DhtCrawlerService>();

// Configure logging
Builder.Logging.ClearProviders();
Builder.Logging.AddConsole();
Builder.Logging.SetMinimumLevel(LogLevel.Information);

IHost AppHost = Builder.Build();
await AppHost.RunAsync();