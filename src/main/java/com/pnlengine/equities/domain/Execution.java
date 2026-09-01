package com.pnlengine.equities.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record Execution(
        String id,
        String accountId,
        String symbol,
        Side side,
        BigDecimal quantity,
        BigDecimal price,
        BigDecimal commission,
        Instant timestamp
) {
    public Execution {
        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than zero");
        }
        if (price == null || price.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Price cannot be negative");
        }
        if (commission == null || commission.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Commission cannot be negative");
        }
    }
}
