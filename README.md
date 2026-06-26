# Shared.Caching

Drop-in Redis caching module implementing the best practices discussed:

| File | Purpose |
|---|---|
| `ICacheService.cs` | Abstraction — keeps the rest of the app decoupled from StackExchange.Redis |
| `CacheKeys.cs` | Centralized, versioned key naming |
| `RedisCacheService.cs` | Implementation: cache-aside, stampede-lock protection, Polly retry + circuit breaker, idempotency guard |
| `CachingServiceCollectionExtensions.cs` | DI registration |
| `Example_GetStockLevelHandler.cs` | Cache-aside read example (InventoryService) |
| `Example_StockChangedConsumer.cs` | Outbox/Kafka-driven invalidation + consumer dedup example |

## Wiring it up (`Program.cs`)

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRedisCaching(builder.Configuration);

// existing registrations...
builder.Services.AddScoped<GetStockLevelHandler>();
builder.Services.AddScoped<StockChangedConsumer>();

var app = builder.Build();
```

## `appsettings.json`

```json
{
  "ConnectionStrings": {
    "Redis": "redis-master:6379,abortConnect=false"
  }
}
```

For production, point this at your Redis Sentinel/Cluster endpoint and consider
enabling `ssl=true,password=...` via configuration/secrets, not inline in source.

## NuGet packages needed

```bash
dotnet add package StackExchange.Redis
dotnet add package Polly.Core
```

## Design decisions baked in (and why)

1. **Cache is never load-bearing for correctness.** Every Redis call is wrapped
   in Polly retry + circuit breaker; on a broken circuit, reads fall back to
   the factory (DB), and idempotency checks fail *open* by default — log
   loudly, but don't halt the pipeline. Revisit fail-open vs fail-closed per
   use case; for payment-adjacent dedup you may want fail-closed instead.
2. **Invalidation is event-driven, not inline.** `StockChangedConsumer` reacts
   to the Outbox-relayed event so every replica's cache gets invalidated
   consistently — an inline `cache.Remove()` inside the command handler only
   clears the local process's view in a multi-pod deployment if you're not
   careful, and races with the DB commit.
3. **Stampede protection uses a short-lived distributed lock** (`SET NX EX`)
   with a bounded polling wait, not an unbounded blocking wait — if the lock
   holder dies, other callers fall through to the DB rather than hanging.
4. **Lock release uses a Lua check-and-delete** so a slow caller can't release
   a lock that's since been acquired by someone else after expiry.
5. **TTLs are always set** — even the "safety net" 24h ones — there is no
   permanent-cache path in this module by design.

## Next steps you might want

- A `RemoveByPrefixAsync` call after warehouse config changes (bumps cover
  multiple SKUs under one warehouse)
- Wrapping `GetOrSetAsync` calls in OpenTelemetry spans tagged with
  `cache.hit`/`cache.miss` for Grafana hit-ratio dashboards
- Swapping `When.NotExists` idempotency for a Redis `SETEX` + Lua script if
  you need atomic check-and-process-result storage in one round trip
