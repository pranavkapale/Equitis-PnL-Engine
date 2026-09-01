package com.pnlengine.equities.domain;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

public record PositionBook(
        String accountId,
        String symbol,
        BigDecimal netQuantity,
        BigDecimal costBasis,
        BigDecimal realizedPnl,
        BigDecimal unrealizedPnl,
        List<TaxLot> openLots
) {
    public PositionBook {
        if (openLots == null) {
            openLots = Collections.emptyList();
        } else {
            openLots = List.copyOf(openLots); // Ensure immutability
        }
        if (netQuantity == null) netQuantity = BigDecimal.ZERO;
        if (costBasis == null) costBasis = BigDecimal.ZERO;
        if (realizedPnl == null) realizedPnl = BigDecimal.ZERO;
        if (unrealizedPnl == null) unrealizedPnl = BigDecimal.ZERO;
    }

    public static PositionBook empty(String accountId, String symbol) {
        return new PositionBook(
                accountId,
                symbol,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                Collections.emptyList()
        );
    }
}
