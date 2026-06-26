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

    /// <summary>
    /// Reads the current value of a version counter (defaults to 1 if it
    /// doesn't exist yet — never returns 0, since 0 makes a poor cache-key
    /// segment to debug against).
    /// </summary>
    Task<long> GetVersionAsync(string versionKey, CancellationToken ct = default);

    /// <summary>
    /// Atomically increments a version counter and returns the new value.
    /// Call this whenever the underlying data a versioned key-space depends
    /// on changes — old cached entries become orphaned and expire via TTL
    /// rather than needing explicit fan-out deletion.
    /// </summary>
    Task<long> BumpVersionAsync(string versionKey, CancellationToken ct = default);
}
