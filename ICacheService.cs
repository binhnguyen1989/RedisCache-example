namespace Shared.Caching;

public interface ICacheService
{
    /// <summary>
    /// Cache-aside read: tries Redis first; on miss, calls <paramref name="factory"/>,
    /// stores the result with <paramref name="ttl"/>, and returns it.
    /// Uses a distributed lock to prevent cache-stampede on hot keys.
    /// </summary>
    Task<T?> GetOrSetAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        TimeSpan ttl,
        CancellationToken ct = default);

    Task<T?> GetAsync<T>(string key, CancellationToken ct = default);

    Task SetAsync<T>(string key, T value, TimeSpan ttl, CancellationToken ct = default);

    Task RemoveAsync(string key, CancellationToken ct = default);

    /// <summary>
    /// Removes all keys matching a prefix (e.g. "inventory:sku:123:*").
    /// Use sparingly — SCAN-based, not O(1).
    /// </summary>
    Task RemoveByPrefixAsync(string prefix, CancellationToken ct = default);

    /// <summary>
    /// Atomic "claim once" check used for idempotent consumers / dedup.
    /// Returns true if this is the first time the key has been seen
    /// (i.e. the caller should proceed); false if it's a duplicate.
    /// </summary>
    Task<bool> TryAcquireOnceAsync(string key, TimeSpan ttl, CancellationToken ct = default);
}
