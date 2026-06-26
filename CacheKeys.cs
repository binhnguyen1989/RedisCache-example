namespace Shared.Caching;

/// <summary>
/// Centralizes key naming so every service builds keys the same way.
/// Versioning lets you invalidate an entire shape of cached data on
/// deploy (bump the version) instead of doing a blanket FLUSHDB.
/// </summary>
public static class CacheKeys
{
    public static string InventoryStock(Guid skuId, int version = 1)
        => $"inventory:sku:{skuId}:stock:v{version}";

    public static string OrderSummary(Guid orderId, int version = 1)
        => $"order:{orderId}:summary:v{version}";

    public static string WarehouseConfig(Guid warehouseId, int version = 1)
        => $"warehouse:{warehouseId}:config:v{version}";

    /// <summary>Key for idempotent Kafka consumer dedup.</summary>
    public static string ConsumerDedup(string consumerGroup, string messageKey)
        => $"idem:{consumerGroup}:{messageKey}";

    /// <summary>Internal lock key used to guard cache-stampede repopulation.</summary>
    public static string LockFor(string cacheKey) => $"lock:{cacheKey}";
}
