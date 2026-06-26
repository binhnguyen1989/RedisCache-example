using Microsoft.Extensions.Logging;
using Shared.Caching;

namespace InventoryService.Application.Queries;

public record GetStockLevelQuery(Guid SkuId);

public record StockLevelDto(Guid SkuId, int QuantityOnHand, int QuantityReserved);

/// <summary>
/// Cache-aside read for hot stock-level lookups. TTL is intentionally short
/// (stock changes often); the safety here comes from the Outbox-driven
/// invalidation in StockChangedConsumer, not from a long TTL.
/// </summary>
public sealed class GetStockLevelHandler
{
    private readonly ICacheService _cache;
    private readonly IInventoryRepository _repository; // your existing EF Core repo
    private readonly ILogger<GetStockLevelHandler> _logger;

    private static readonly TimeSpan StockTtl = TimeSpan.FromSeconds(45);

    public GetStockLevelHandler(
        ICacheService cache,
        IInventoryRepository repository,
        ILogger<GetStockLevelHandler> logger)
    {
        _cache = cache;
        _repository = repository;
        _logger = logger;
    }

    public async Task<StockLevelDto?> HandleAsync(GetStockLevelQuery query, CancellationToken ct)
    {
        var key = CacheKeys.InventoryStock(query.SkuId);

        return await _cache.GetOrSetAsync<StockLevelDto>(
            key,
            async token =>
            {
                _logger.LogDebug("Cache miss for {SkuId}, loading from DB", query.SkuId);

                var stock = await _repository.GetStockAsync(query.SkuId, token);
                return stock is null
                    ? null
                    : new StockLevelDto(stock.SkuId, stock.QuantityOnHand, stock.QuantityReserved);
            },
            StockTtl,
            ct);
    }
}

/// <summary>Minimal contract for the existing repository — adjust to your actual interface.</summary>
public interface IInventoryRepository
{
    Task<StockSnapshot?> GetStockAsync(Guid skuId, CancellationToken ct);
}

public record StockSnapshot(Guid SkuId, int QuantityOnHand, int QuantityReserved);
