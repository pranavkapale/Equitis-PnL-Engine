package com.pnlengine.equities.engine;

import com.pnlengine.equities.domain.Execution;
import com.pnlengine.equities.domain.PositionBook;
import com.pnlengine.equities.domain.Side;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class PositionEngineTest {

    private PositionEngine engine;

    @BeforeEach
    void setUp() {
        engine = new PositionEngine();
    }

    @ParameterizedTest
    @EnumSource(LotMatchingStrategy.class)
    void testSimpleAccumulation(LotMatchingStrategy strategy) {
        PositionBook initialBook = PositionBook.empty("ACC-1", "AAPL");

        Execution exec1 = new Execution("1", "ACC-1", "AAPL", Side.BUY, new BigDecimal("10"), new BigDecimal("100"), BigDecimal.ZERO, Instant.now());
        PositionBook book1 = engine.applyExecution(initialBook, exec1, strategy);

        assertThat(book1.netQuantity()).isEqualByComparingTo("10");
        assertThat(book1.costBasis()).isEqualByComparingTo("100");
        assertThat(book1.realizedPnl()).isEqualByComparingTo("0");
        assertThat(book1.unrealizedPnl()).isEqualByComparingTo("0");

        Execution exec2 = new Execution("2", "ACC-1", "AAPL", Side.BUY, new BigDecimal("10"), new BigDecimal("110"), BigDecimal.ZERO, Instant.now());
        PositionBook book2 = engine.applyExecution(book1, exec2, strategy);

        assertThat(book2.netQuantity()).isEqualByComparingTo("20");
        assertThat(book2.costBasis()).isEqualByComparingTo("105"); // WAC
        assertThat(book2.unrealizedPnl()).isEqualByComparingTo("100"); // (110 - 105) * 20
        
        if (strategy == LotMatchingStrategy.FIFO) {
            assertThat(book2.openLots()).hasSize(2);
        } else {
            assertThat(book2.openLots()).hasSize(1);
        }
    }

    @Test
    void testPartialLiquidationFIFO() {
        PositionBook initialBook = PositionBook.empty("ACC-1", "AAPL");

        Execution exec1 = new Execution("1", "ACC-1", "AAPL", Side.BUY, new BigDecimal("10"), new BigDecimal("100"), BigDecimal.ZERO, Instant.now());
        PositionBook book1 = engine.applyExecution(initialBook, exec1, LotMatchingStrategy.FIFO);

        Execution exec2 = new Execution("2", "ACC-1", "AAPL", Side.BUY, new BigDecimal("10"), new BigDecimal("110"), BigDecimal.ZERO, Instant.now());
        PositionBook book2 = engine.applyExecution(book1, exec2, LotMatchingStrategy.FIFO);

        // Sell 15 shares at 120
        Execution sellExec = new Execution("3", "ACC-1", "AAPL", Side.SELL, new BigDecimal("15"), new BigDecimal("120"), BigDecimal.ZERO, Instant.now());
        PositionBook book3 = engine.applyExecution(book2, sellExec, LotMatchingStrategy.FIFO);

        assertThat(book3.netQuantity()).isEqualByComparingTo("5");
        assertThat(book3.costBasis()).isEqualByComparingTo("110"); // Only the second lot remains
        assertThat(book3.openLots()).hasSize(1);
        assertThat(book3.openLots().get(0).quantity()).isEqualByComparingTo("5");
        assertThat(book3.openLots().get(0).price()).isEqualByComparingTo("110");

        // Realized PnL:
        // Sold 10 from exec1 (cost 100) -> Proceeds: 1200, Cost: 1000 -> PnL: 200
        // Sold 5 from exec2 (cost 110) -> Proceeds: 600, Cost: 550 -> PnL: 50
        // Total Realized = 250
        assertThat(book3.realizedPnl()).isEqualByComparingTo("250");
        
        // Unrealized PnL: (120 - 110) * 5 = 50
        assertThat(book3.unrealizedPnl()).isEqualByComparingTo("50");
    }

    @ParameterizedTest
    @EnumSource(LotMatchingStrategy.class)
    void testCrossZeroPosition(LotMatchingStrategy strategy) {
        PositionBook initialBook = PositionBook.empty("ACC-1", "AAPL");

        Execution exec1 = new Execution("1", "ACC-1", "AAPL", Side.BUY, new BigDecimal("10"), new BigDecimal("100"), BigDecimal.ZERO, Instant.now());
        PositionBook book1 = engine.applyExecution(initialBook, exec1, strategy);

        // Sell 15 shares at 120
        Execution sellExec = new Execution("2", "ACC-1", "AAPL", Side.SELL, new BigDecimal("15"), new BigDecimal("120"), BigDecimal.ZERO, Instant.now());
        PositionBook book2 = engine.applyExecution(book1, sellExec, strategy);

        assertThat(book2.netQuantity()).isEqualByComparingTo("-5");
        assertThat(book2.costBasis()).isEqualByComparingTo("120"); // Short cost basis is 120
        
        // Realized PnL: closing 10 long shares at 120 (cost 100) -> 200
        assertThat(book2.realizedPnl()).isEqualByComparingTo("200");
        
        // Unrealized PnL: 0 because current price matches cost basis
        assertThat(book2.unrealizedPnl()).isEqualByComparingTo("0");
    }

    @Test
    void testCommissionImpact() {
        PositionBook initialBook = PositionBook.empty("ACC-1", "AAPL");

        // Buy 10 at 100, comm 10
        // Total Cost = 1000 + 10 = 1010
        // Cost Basis = 101
        Execution exec1 = new Execution("1", "ACC-1", "AAPL", Side.BUY, new BigDecimal("10"), new BigDecimal("100"), new BigDecimal("10"), Instant.now());
        PositionBook book1 = engine.applyExecution(initialBook, exec1, LotMatchingStrategy.FIFO);

        assertThat(book1.costBasis()).isEqualByComparingTo("101");
        
        // Sell 10 at 120, comm 15
        // Proceeds = 1200
        // PnL before close comm = 1200 - 1010 = 190
        // Final PnL = 190 - 15 = 175
        Execution sellExec = new Execution("2", "ACC-1", "AAPL", Side.SELL, new BigDecimal("10"), new BigDecimal("120"), new BigDecimal("15"), Instant.now());
        PositionBook book2 = engine.applyExecution(book1, sellExec, LotMatchingStrategy.FIFO);

        assertThat(book2.netQuantity()).isEqualByComparingTo("0");
        assertThat(book2.realizedPnl()).isEqualByComparingTo("175");
    }
}
