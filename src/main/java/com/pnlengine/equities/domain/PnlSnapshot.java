package com.pnlengine.equities.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record PnlSnapshot(
        String accountId,
        String symbol,
        BigDecimal deltaRealizedPnl,
        BigDecimal totalRealizedPnl,
        Instant timestamp
) {
}
