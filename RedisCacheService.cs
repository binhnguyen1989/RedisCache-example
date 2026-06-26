using System.Text.Json;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using StackExchange.Redis;

namespace Shared.Caching;

/// <summary>
/// StackExchange.Redis-backed cache service.
/// - Cache-aside with distributed-lock stampede protection on hot keys.
/// - Resilient: Redis outages degrade to calling the factory directly
///   (cache is never a hard dependency for correctness).
/// - System.Text.Json serialization.
///
/// FAILURE HANDLING CONTRACT: no method on this class throws a Redis-origin
/// exception. Every public operation is wrapped in <see cref="SafeExecuteAsync"/>,
/// which catches the full StackExchange.Redis exception surface (connection
/// drops, timeouts, server-side errors like "Internal server error", and an
/// open circuit) and degrades to a safe fallback value instead of propagating.
/// This is what stands between a Redis blip and a 500 reaching your callers.
/// </summary>
public sealed class RedisCacheService : ICacheService
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly ILogger<RedisCacheService> _logger;
    private readonly ResiliencePipeline _resiliencePipeline;
    private readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

    private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan LockWaitTotal = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan LockRetryDelay = TimeSpan.FromMilliseconds(100);

    public RedisCacheService(
        IConnectionMultiplexer multiplexer,
        ILogger<RedisCacheService> logger)
    {
        _multiplexer = multiplexer;
        _logger = logger;

        // Circuit breaker + retry with jitter: Redis being flaky should never
        // take the calling service down with it.
        //
        // RedisException is the common base for RedisConnectionException,
        // RedisTimeoutException, AND RedisServerException (what an actual
        // "Internal server error" from Redis itself throws). Handling only
        // the first two — as an earlier version of this class did — leaves
        // server-side errors completely unretried and uncaught.
        _resiliencePipeline = new ResiliencePipelineBuilder()
            .AddRetry(new Polly.Retry.RetryStrategyOptions
            {
                MaxRetryAttempts = 2,
                Delay = TimeSpan.FromMilliseconds(50),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<RedisException>()
                                                      .Handle<TimeoutException>()
            })
            .AddCircuitBreaker(new Polly.CircuitBreaker.CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                SamplingDuration = TimeSpan.FromSeconds(30),
                MinimumThroughput = 10,
                BreakDuration = TimeSpan.FromSeconds(15),
                ShouldHandle = new PredicateBuilder().Handle<RedisException>()
                                                      .Handle<TimeoutException>()
            })
            .Build();
    }

    private IDatabase Db => _multiplexer.GetDatabase();

    /// <summary>
    /// Single chokepoint for "Redis failed, degrade gracefully" behavior.
    /// Every public method routes through this so failure handling can't be
    /// forgotten or partially applied on a future new method.
    /// </summary>
    private async Task<T> SafeExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        T fallback,
        string operationName,
        string key,
        CancellationToken ct)
    {
        try
        {
            return await _resiliencePipeline.ExecuteAsync(operation, ct);
        }
        catch (BrokenCircuitException)
        {
            _logger.LogWarning(
                "Redis circuit open during {Operation} for {Key}; using fallback", operationName, key);
            return fallback;
        }
        catch (RedisException ex)
        {
            // Covers RedisServerException ("Internal server error" and similar
            // server-side faults), RedisConnectionException, and any
            // RedisTimeoutException that slipped through after retries
            // were exhausted but before the breaker tripped.
            _logger.LogWarning(ex,
                "Redis error during {Operation} for {Key}; using fallback", operationName, key);
            return fallback;
        }
        catch (TimeoutException ex)
        {
            // StackExchange.Redis can also surface a plain TimeoutException
            // (e.g. sync-over-async timeouts) outside the RedisException tree.
            _logger.LogWarning(ex,
                "Timeout during {Operation} for {Key}; using fallback", operationName, key);
            return fallback;
        }
    }

    public Task<T?> GetAsync<T>(string key, CancellationToken ct = default) =>
        SafeExecuteAsync(
            async token =>
            {
                var value = await Db.StringGetAsync(key);
                return value.IsNullOrEmpty ? default : JsonSerializer.Deserialize<T>(value!, _jsonOptions);
            },
            fallback: default,
            operationName: nameof(GetAsync),
            key: key,
            ct);

    public Task SetAsync<T>(string key, T value, TimeSpan ttl, CancellationToken ct = default) =>
        SafeExecuteAsync(
            async token =>
            {
                var payload = JsonSerializer.Serialize(value, _jsonOptions);
                await Db.StringSetAsync(key, payload, ttl);
                return true; // SafeExecuteAsync needs a T; unused by caller
            },
            fallback: false,
            operationName: nameof(SetAsync),
            key: key,
            ct);

    public Task RemoveAsync(string key, CancellationToken ct = default) =>
        SafeExecuteAsync(
            async token =>
            {
                await Db.KeyDeleteAsync(key);
                return true;
            },
            fallback: false,
            operationName: nameof(RemoveAsync),
            key: key,
            ct);

    public async Task RemoveByPrefixAsync(string prefix, CancellationToken ct = default)
    {
        await SafeExecuteAsync(
            async token =>
            {
                var endpoints = _multiplexer.GetEndPoints();
                foreach (var endpoint in endpoints)
                {
                    var server = _multiplexer.GetServer(endpoint);
                    if (!server.IsConnected) continue;

                    // SCAN-based — non-blocking, safe for production unlike KEYS.
                    await foreach (var key in server.KeysAsync(pattern: $"{prefix}*"))
                    {
                        if (token.IsCancellationRequested) break;
                        await Db.KeyDeleteAsync(key);
                    }
                }
                return true;
            },
            fallback: false,
            operationName: nameof(RemoveByPrefixAsync),
            key: prefix,
            ct);
    }

    public async Task<T?> GetOrSetAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        TimeSpan ttl,
        CancellationToken ct = default)
    {
        // 1. Fast path: cache hit.
        var cached = await GetAsync<T>(key, ct);
        if (cached is not null)
            return cached;

        // 2. Miss — try to take the lock so only one caller repopulates.
        var lockKey = CacheKeys.LockFor(key);
        var lockToken = Guid.NewGuid().ToString("N");
        var acquired = await TryAcquireLockAsync(lockKey, lockToken, ct);

        if (!acquired)
        {
            // Someone else is repopulating (or Redis is unavailable and the
            // lock attempt silently failed — see TryAcquireLockAsync). Either
            // way, briefly poll instead of all hammering the DB at once.
            var deadline = DateTime.UtcNow + LockWaitTotal;
            while (DateTime.UtcNow < deadline)
            {
                await Task.Delay(LockRetryDelay, ct);
                cached = await GetAsync<T>(key, ct);
                if (cached is not null)
                    return cached;
            }

            // Lock holder is slow/dead, or Redis is down — fall through and
            // hit the DB ourselves. Correctness > strict single-flight.
        }

        // NOTE: this part calls the user-supplied factory (typically a DB
        // query), not Redis — its exceptions are the caller's domain
        // exceptions and are intentionally NOT swallowed here.
        try
        {
            var value = await factory(ct);
            if (value is not null)
                await SetAsync(key, value, ttl, ct); // already fail-safe

            return value;
        }
        finally
        {
            if (acquired)
                await ReleaseLockAsync(lockKey, lockToken);
        }
    }

    public Task<bool> TryAcquireOnceAsync(string key, TimeSpan ttl, CancellationToken ct = default) =>
        SafeExecuteAsync(
            async token => await Db.StringSetAsync(key, "1", ttl, When.NotExists),
            // Fail-open vs fail-closed is a judgment call. For consumer dedup,
            // failing open (allow processing) is usually safer than blocking
            // the pipeline — but the warning above is logged so you can
            // investigate. Flip to `fallback: false` for fail-closed callers
            // (e.g. payment dedup) by injecting a second strategy if needed.
            fallback: true,
            operationName: nameof(TryAcquireOnceAsync),
            key: key,
            ct);

    public Task<long> GetVersionAsync(string versionKey, CancellationToken ct = default) =>
        SafeExecuteAsync(
            async token =>
            {
                var value = await Db.StringGetAsync(versionKey);
                return value.IsNullOrEmpty ? 1 : (long)value;
            },
            fallback: 1L, // un-versioned baseline; matches BumpVersionAsync's first INCR result
            operationName: nameof(GetVersionAsync),
            key: versionKey,
            ct);

    public Task<long> BumpVersionAsync(string versionKey, CancellationToken ct = default) =>
        SafeExecuteAsync(
            async token => await Db.StringIncrementAsync(versionKey),
            // Returning a sentinel here (rather than throwing, as an earlier
            // version did) keeps this consistent with every other method:
            // Redis failures degrade, they don't propagate. Callers that
            // build cache keys from this should treat -1 as "don't trust
            // this version" — in practice CachedQueryService just uses it
            // as a normal key segment, so worst case is a brief miss, never
            // a thrown exception.
            fallback: -1L,
            operationName: nameof(BumpVersionAsync),
            key: versionKey,
            ct);

    private Task<bool> TryAcquireLockAsync(string lockKey, string token, CancellationToken ct) =>
        SafeExecuteAsync(
            async t => await Db.StringSetAsync(lockKey, token, LockTimeout, When.NotExists),
            fallback: false, // if Redis is down, treat as "couldn't get lock" — caller falls through to DB
            operationName: nameof(TryAcquireLockAsync),
            key: lockKey,
            ct);

    private async Task ReleaseLockAsync(string lockKey, string token)
    {
        // Only release if we still own it — classic check-and-delete via Lua
        // to avoid releasing a lock acquired by someone else after expiry.
        const string script = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
            """;

        await SafeExecuteAsync(
            async token =>
            {
                await Db.ScriptEvaluateAsync(script, new RedisKey[] { lockKey }, new RedisValue[] { token });
                return true;
            },
            fallback: false, // lock will expire naturally via its own TTL — not a correctness issue
            operationName: nameof(ReleaseLockAsync),
            key: lockKey,
            CancellationToken.None);
    }
}
