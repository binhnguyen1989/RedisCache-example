using Microsoft.Extensions.Logging;
using Shared.Caching;

namespace InventoryService.Infrastructure.Consumers;

public record StockChangedEvent(Guid SkuId, int NewQuantityOnHand, string EventId);

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
    private readonly ILogger<StockChangedConsumer> _logger;

    private const string ConsumerGroup = "inventory-cache-invalidator";
    private static readonly TimeSpan DedupWindow = TimeSpan.FromHours(24);

    public StockChangedConsumer(ICacheService cache, ILogger<StockChangedConsumer> logger)
    {
        _cache = cache;
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

        var cacheKey = CacheKeys.InventoryStock(@event.SkuId);
        await _cache.RemoveAsync(cacheKey, ct);

        _logger.LogInformation(
            "Invalidated stock cache for SKU {SkuId} after {EventId}", @event.SkuId, @event.EventId);

        // Note: we invalidate rather than write-through here. The next read
        // will repopulate via cache-aside with the stampede-safe lock.
        // Write-through is tempting (`SetAsync` with NewQuantityOnHand`),
        // but it risks caching a value that arrived out of order relative
        // to a concurrent DB write. Invalidate + re-read from the
        // source of truth is the safer default.
    }
}
