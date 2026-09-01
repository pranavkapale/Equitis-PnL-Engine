package com.pnlengine.equities.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record TaxLot(
        String id,
        BigDecimal quantity,
        BigDecimal price,
        Instant timestamp
) {
    public TaxLot {
        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than zero");
        }
        if (price == null || price.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Price cannot be negative");
        }
    }

    public TaxLot withQuantity(BigDecimal newQuantity) {
        return new TaxLot(this.id, newQuantity, this.price, this.timestamp);
    }
}
