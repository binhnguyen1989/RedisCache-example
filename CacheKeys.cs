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

    /// <summary>
    /// Version counter for a warehouse scope. Bump this whenever stock
    /// changes within the warehouse; every list/aggregate query cached
    /// against the old version is orphaned and expires via TTL.
    /// Used with <see cref="CachedQueryService"/>.
    /// </summary>
    public static string WarehouseVersion(Guid warehouseId)
        => $"warehouse:{warehouseId}:version";

    /// <summary>Unversioned query-shape key — CachedQueryService appends ":v{n}".</summary>
    public static string LowStockSkusShape(Guid warehouseId, int threshold)
        => $"inventory:warehouse:{warehouseId}:low-stock:{threshold}";

    /// <summary>Key for idempotent Kafka consumer dedup.</summary>
    public static string ConsumerDedup(string consumerGroup, string messageKey)
        => $"idem:{consumerGroup}:{messageKey}";

    /// <summary>Internal lock key used to guard cache-stampede repopulation.</summary>
    public static string LockFor(string cacheKey) => $"lock:{cacheKey}";
}
