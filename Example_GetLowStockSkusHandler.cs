using Microsoft.Extensions.Logging;
using Shared.Caching;

namespace InventoryService.Application.Queries;

public record GetLowStockSkusQuery(Guid WarehouseId, int Threshold);

/// <summary>
/// List query cached with the version-counter pattern. Unlike
/// GetStockLevelHandler (single-entity, direct key removal), invalidating
/// this one explicitly per write would require tracking every cached
/// threshold variant ever requested. Instead, WarehouseStockConsumer bumps
/// the warehouse's version counter on any stock change, which silently
/// orphans every previously-cached query for that warehouse.
/// </summary>
public sealed class GetLowStockSkusHandler
{
    private readonly CachedQueryService _cachedQuery;
    private readonly IInventoryRepository _repository;
    private readonly ILogger<GetLowStockSkusHandler> _logger;

    // Short TTL is load-bearing here: orphaned (stale-version) entries only
    // leave Redis memory once their TTL expires, so keep this tight.
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);

    public GetLowStockSkusHandler(
        CachedQueryService cachedQuery,
        IInventoryRepository repository,
        ILogger<GetLowStockSkusHandler> logger)
    {
        _cachedQuery = cachedQuery;
        _repository = repository;
        _logger = logger;
    }

    public async Task<List<StockLevelDto>> HandleAsync(GetLowStockSkusQuery query, CancellationToken ct)
    {
        var scopeVersionKey = CacheKeys.WarehouseVersion(query.WarehouseId);
        var shapeKey = CacheKeys.LowStockSkusShape(query.WarehouseId, query.Threshold);

        var result = await _cachedQuery.GetOrSetAsync<List<StockLevelDto>>(
            scopeVersionKey,
            shapeKey,
            async token =>
            {
                _logger.LogDebug(
                    "Cache miss for low-stock query, warehouse {WarehouseId} threshold {Threshold}",
                    query.WarehouseId, query.Threshold);

                return await _repository.GetLowStockAsync(query.WarehouseId, query.Threshold, token);
            },
            Ttl,
            ct);

        return result ?? new List<StockLevelDto>();
    }
}
