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
        _resiliencePipeline = new ResiliencePipelineBuilder()
            .AddRetry(new Polly.Retry.RetryStrategyOptions
            {
                MaxRetryAttempts = 2,
                Delay = TimeSpan.FromMilliseconds(50),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<RedisConnectionException>()
                                                      .Handle<RedisTimeoutException>()
            })
            .AddCircuitBreaker(new Polly.CircuitBreaker.CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                SamplingDuration = TimeSpan.FromSeconds(30),
                MinimumThroughput = 10,
                BreakDuration = TimeSpan.FromSeconds(15),
                ShouldHandle = new PredicateBuilder().Handle<RedisConnectionException>()
                                                      .Handle<RedisTimeoutException>()
            })
            .Build();
    }

    private IDatabase Db => _multiplexer.GetDatabase();

    public async Task<T?> GetAsync<T>(string key, CancellationToken ct = default)
    {
        try
        {
            var value = await _resiliencePipeline.ExecuteAsync(
                async token => await Db.StringGetAsync(key), ct);

            if (value.IsNullOrEmpty)
                return default;

            return JsonSerializer.Deserialize<T>(value!, _jsonOptions);
        }
        catch (BrokenCircuitException)
        {
            _logger.LogWarning("Redis circuit open; treating {Key} as a miss", key);
            return default;
        }
    }

    public async Task SetAsync<T>(string key, T value, TimeSpan ttl, CancellationToken ct = default)
    {
        try
        {
            var payload = JsonSerializer.Serialize(value, _jsonOptions);
            await _resiliencePipeline.ExecuteAsync(
                async token => await Db.StringSetAsync(key, payload, ttl), ct);
        }
        catch (BrokenCircuitException)
        {
            _logger.LogWarning("Redis circuit open; skipped caching {Key}", key);
        }
    }

    public async Task RemoveAsync(string key, CancellationToken ct = default)
    {
        try
        {
            await _resiliencePipeline.ExecuteAsync(
                async token => await Db.KeyDeleteAsync(key), ct);
        }
        catch (BrokenCircuitException)
        {
            _logger.LogWarning("Redis circuit open; could not invalidate {Key}", key);
        }
    }

    public async Task RemoveByPrefixAsync(string prefix, CancellationToken ct = default)
    {
        try
        {
            var endpoints = _multiplexer.GetEndPoints();
            foreach (var endpoint in endpoints)
            {
                var server = _multiplexer.GetServer(endpoint);
                if (!server.IsConnected) continue;

                // SCAN-based — non-blocking, safe for production unlike KEYS.
                await foreach (var key in server.KeysAsync(pattern: $"{prefix}*"))
                {
                    if (ct.IsCancellationRequested) break;
                    await Db.KeyDeleteAsync(key);
                }
            }
        }
        catch (RedisConnectionException ex)
        {
            _logger.LogWarning(ex, "Redis unavailable; could not remove keys by prefix {Prefix}", prefix);
        }
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
            // Someone else is repopulating. Briefly poll the cache instead of
            // all hammering the DB at once (this is the stampede protection).
            var deadline = DateTime.UtcNow + LockWaitTotal;
            while (DateTime.UtcNow < deadline)
            {
                await Task.Delay(LockRetryDelay, ct);
                cached = await GetAsync<T>(key, ct);
                if (cached is not null)
                    return cached;
            }

            // Lock holder is slow/dead — fall through and hit the DB ourselves
            // rather than waiting forever. Correctness > strict single-flight.
        }

        try
        {
            var value = await factory(ct);
            if (value is not null)
                await SetAsync(key, value, ttl, ct);

            return value;
        }
        finally
        {
            if (acquired)
                await ReleaseLockAsync(lockKey, lockToken);
        }
    }

    public async Task<bool> TryAcquireOnceAsync(string key, TimeSpan ttl, CancellationToken ct = default)
    {
        try
        {
            // SET key value NX EX ttl — atomic, single round trip.
            // True => first time seen, caller should proceed.
            // False => duplicate, caller should skip.
            return await _resiliencePipeline.ExecuteAsync(
                async token => await Db.StringSetAsync(key, "1", ttl, When.NotExists), ct);
        }
        catch (BrokenCircuitException)
        {
            // Fail-open vs fail-closed is a judgment call. For consumer dedup,
            // failing open (allow processing) is usually safer than blocking
            // the pipeline — but log loudly so you can investigate.
            _logger.LogError("Redis circuit open during idempotency check for {Key}; failing open", key);
            return true;
        }
    }

    private async Task<bool> TryAcquireLockAsync(string lockKey, string token, CancellationToken ct)
    {
        try
        {
            return await _resiliencePipeline.ExecuteAsync(
                async t => await Db.StringSetAsync(lockKey, token, LockTimeout, When.NotExists), ct);
        }
        catch (BrokenCircuitException)
        {
            return false;
        }
    }

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

        try
        {
            await Db.ScriptEvaluateAsync(script, new RedisKey[] { lockKey }, new RedisValue[] { token });
        }
        catch (RedisConnectionException ex)
        {
            _logger.LogWarning(ex, "Failed to release lock {LockKey}; it will expire naturally", lockKey);
        }
    }
}
