using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace Shared.Caching;

public static class CachingServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton IConnectionMultiplexer plus ICacheService.
    /// Connection string comes from configuration: "ConnectionStrings:Redis".
    /// </summary>
    public static IServiceCollection AddRedisCaching(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddSingleton<IConnectionMultiplexer>(_ =>
        {
            var connectionString = configuration.GetConnectionString("Redis")
                ?? throw new InvalidOperationException(
                    "Missing 'Redis' connection string in configuration.");

            var options = ConfigurationOptions.Parse(connectionString);
            options.AbortOnConnectFail = false; // keep retrying instead of crashing on startup
            options.ConnectRetry = 3;
            options.ReconnectRetryPolicy = new ExponentialRetry(1000);

            return ConnectionMultiplexer.Connect(options);
        });

        services.AddSingleton<ICacheService, RedisCacheService>();
        services.AddSingleton<CachedQueryService>();

        return services;
    }
}
