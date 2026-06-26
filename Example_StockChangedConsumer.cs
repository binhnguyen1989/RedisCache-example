using Microsoft.Extensions.Logging;
using Shared.Caching;

namespace InventoryService.Infrastructure.Consumers;

public record StockChangedEvent(Guid SkuId, Guid WarehouseId, int NewQuantityOnHand, string EventId);

/// <summary>
/// Consumes domain events relayed from the Outbox (e.g. StockChangedEvent)
/// and invalidates the corresponding Redis cache entry. This keeps cache
/// invalidation consistent across every InventoryService replica, instead
/// of invalidating inline inside the command handler that wrote the change
/// (which would miss other pods' caches).
///
/// Also demonstrates idempotent-consumer dedup using TryAcquireOnceAsync,
/// so re-delivered Kafka messages (at-least-once semantics) don't cause
/// duplicate side effects.
/// </summary>
public sealed class StockChangedConsumer
{
    private readonly ICacheService _cache;
    private readonly CachedQueryService _cachedQuery;
    private readonly ILogger<StockChangedConsumer> _logger;

    private const string ConsumerGroup = "inventory-cache-invalidator";
    private static readonly TimeSpan DedupWindow = TimeSpan.FromHours(24);

    public StockChangedConsumer(
        ICacheService cache,
        CachedQueryService cachedQuery,
        ILogger<StockChangedConsumer> logger)
    {
        _cache = cache;
        _cachedQuery = cachedQuery;
        _logger = logger;
    }

    public async Task HandleAsync(StockChangedEvent @event, CancellationToken ct)
    {
        var dedupKey = CacheKeys.ConsumerDedup(ConsumerGroup, @event.EventId);

        var isNew = await _cache.TryAcquireOnceAsync(dedupKey, DedupWindow, ct);
        if (!isNew)
        {
            _logger.LogInformation(
                "Duplicate delivery of {EventId} for SKU {SkuId}; skipping", @event.EventId, @event.SkuId);
            return;
        }

        // 1. Single-entity cache: direct key removal (cheap, precise).
        var cacheKey = CacheKeys.InventoryStock(@event.SkuId);
        await _cache.RemoveAsync(cacheKey, ct);

        // 2. List/aggregate caches scoped to this warehouse: bump the
        //    version counter instead of trying to enumerate every cached
        //    threshold/filter combination that might be affected.
        var scopeVersionKey = CacheKeys.WarehouseVersion(@event.WarehouseId);
        var newVersion = await _cachedQuery.InvalidateScopeAsync(scopeVersionKey, ct);

        if (newVersion < 0)
        {
            // Redis was unavailable for the bump (see RedisCacheService's
            // fail-safe contract). List-query caches for this warehouse may
            // serve stale data until their TTL expires — acceptable given
            // those TTLs are already short (~30s), but worth a loud log.
            _logger.LogError(
                "Could not bump version for warehouse {WarehouseId} after {EventId}; " +
                "low-stock/list caches may be briefly stale", @event.WarehouseId, @event.EventId);
        }
        else
        {
            _logger.LogInformation(
                "Invalidated stock cache for SKU {SkuId} and bumped warehouse {WarehouseId} to v{Version} after {EventId}",
                @event.SkuId, @event.WarehouseId, newVersion, @event.EventId);
        }

        // Note: we invalidate rather than write-through here. The next read
        // will repopulate via cache-aside with the stampede-safe lock.
        // Write-through is tempting (`SetAsync` with NewQuantityOnHand`),
        // but it risks caching a value that arrived out of order relative
        // to a concurrent DB write. Invalidate + re-read from the
        // source of truth is the safer default.
    }
}
