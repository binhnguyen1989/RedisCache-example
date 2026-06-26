namespace Shared.Caching;

/// <summary>
/// Helper for caching list/filter/aggregate query results using the
/// version-counter pattern (Option C from the design discussion):
///
///   1. Each "scope" (e.g. a warehouse, a customer, a tenant) has a version
///      counter in Redis: "warehouse:{id}:version".
///   2. Every cache key for queries within that scope embeds the current
///      version: "inventory:warehouse:{id}:v{version}:low-stock:{threshold}".
///   3. When the underlying data changes, you call BumpVersionAsync once.
///      Every previously-cached key for that scope is now orphaned —
///      nothing reads it again — and it simply expires via its own TTL.
///
/// This avoids SCAN-based fan-out deletion and avoids needing a separate
/// "set of keys to invalidate" bookkeeping structure. The tradeoff: orphaned
/// entries sit in Redis memory until TTL eviction, so keep TTLs short
/// (seconds, not hours) for anything wrapped in this helper.
/// </summary>
public sealed class CachedQueryService
{
    private readonly ICacheService _cache;

    public CachedQueryService(ICacheService cache)
    {
        _cache = cache;
    }

    /// <summary>
    /// Cache-aside read for a query scoped to a versioned aggregate (e.g. a
    /// warehouse). <paramref name="scopeVersionKey"/> should usually come
    /// from <see cref="CacheKeys"/> — see CacheKeys.WarehouseVersion below.
    /// </summary>
    public async Task<T?> GetOrSetAsync<T>(
        string scopeVersionKey,
        string queryShapeKey,
        Func<CancellationToken, Task<T?>> factory,
        TimeSpan ttl,
        CancellationToken ct = default)
    {
        var version = await _cache.GetVersionAsync(scopeVersionKey, ct);
        var fullKey = $"{queryShapeKey}:v{version}";

        return await _cache.GetOrSetAsync(fullKey, factory, ttl, ct);
    }

    /// <summary>
    /// Call from your Outbox/Kafka consumer whenever data within a scope
    /// changes. Every list/aggregate query cached under the old version
    /// becomes unreachable immediately — no explicit deletes required.
    /// </summary>
    public Task<long> InvalidateScopeAsync(string scopeVersionKey, CancellationToken ct = default)
        => _cache.BumpVersionAsync(scopeVersionKey, ct);
}
