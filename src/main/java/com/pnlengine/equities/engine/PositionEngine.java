package com.pnlengine.equities.engine;

import com.pnlengine.equities.domain.Execution;
import com.pnlengine.equities.domain.PositionBook;
import com.pnlengine.equities.domain.Side;
import com.pnlengine.equities.domain.TaxLot;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class PositionEngine {

    private static final MathContext MC = MathContext.DECIMAL128;

    public PositionBook applyExecution(PositionBook state, Execution execution, LotMatchingStrategy strategy) {
        if (!state.accountId().equals(execution.accountId()) || !state.symbol().equals(execution.symbol())) {
            throw new IllegalArgumentException("Execution does not match PositionBook account or symbol");
        }

        BigDecimal execQty = execution.quantity();
        BigDecimal currentNet = state.netQuantity();
        
        boolean isLong = currentNet.compareTo(BigDecimal.ZERO) > 0;
        boolean isShort = currentNet.compareTo(BigDecimal.ZERO) < 0;
        boolean isFlat = currentNet.compareTo(BigDecimal.ZERO) == 0;

        boolean isAccumulating = isFlat ||
                (isLong && execution.side() == Side.BUY) ||
                (isShort && execution.side() == Side.SELL);

        if (isAccumulating) {
            return accumulate(state, execution, strategy);
        }

        BigDecimal absNet = currentNet.abs();
        if (execQty.compareTo(absNet) > 0) {
            // Zero-crossing
            BigDecimal liqQty = absNet;
            BigDecimal accQty = execQty.subtract(absNet);

            BigDecimal execQtyDec = execQty;
            BigDecimal liqRatio = liqQty.divide(execQtyDec, MC);
            BigDecimal accRatio = accQty.divide(execQtyDec, MC);
            BigDecimal liqComm = execution.commission().multiply(liqRatio, MC);
            BigDecimal accComm = execution.commission().multiply(accRatio, MC);

            Execution liqExec = new Execution(
                    execution.id() + "-LIQ", execution.accountId(), execution.symbol(),
                    execution.side(), liqQty, execution.price(), liqComm, execution.timestamp()
            );
            Execution accExec = new Execution(
                    execution.id() + "-ACC", execution.accountId(), execution.symbol(),
                    execution.side(), accQty, execution.price(), accComm, execution.timestamp()
            );

            PositionBook stateAfterLiq = liquidate(state, liqExec, strategy);
            return accumulate(stateAfterLiq, accExec, strategy);
        } else {
            // Pure liquidation
            return liquidate(state, execution, strategy);
        }
    }

    private PositionBook accumulate(PositionBook state, Execution execution, LotMatchingStrategy strategy) {
        BigDecimal execQty = execution.quantity();
        BigDecimal execPrice = execution.price();
        BigDecimal commission = execution.commission();
        
        BigDecimal executionTotalCost;
        if (execution.side() == Side.BUY) {
            executionTotalCost = execQty.multiply(execPrice, MC).add(commission, MC);
        } else {
            executionTotalCost = execQty.multiply(execPrice, MC).subtract(commission, MC);
        }
        
        BigDecimal oldAbsNet = state.netQuantity().abs();
        BigDecimal oldTotalCost = oldAbsNet.multiply(state.costBasis(), MC);
        
        BigDecimal newAbsNet = oldAbsNet.add(execQty, MC);
        BigDecimal newTotalCost = oldTotalCost.add(executionTotalCost, MC);
        BigDecimal newCostBasis = newTotalCost.divide(newAbsNet, MC);

        List<TaxLot> newLots = new ArrayList<>();
        if (strategy == LotMatchingStrategy.WAC) {
            newLots.add(new TaxLot(
                    execution.id(),
                    newAbsNet,
                    newCostBasis,
                    execution.timestamp()
            ));
        } else {
            newLots.addAll(state.openLots());
            BigDecimal lotUnitPrice = executionTotalCost.divide(execQty, MC);
            newLots.add(new TaxLot(
                    execution.id(),
                    execQty,
                    lotUnitPrice,
                    execution.timestamp()
            ));
        }
        
        BigDecimal newNetQuantity = execution.side() == Side.BUY 
                ? state.netQuantity().add(execQty, MC) 
                : state.netQuantity().subtract(execQty, MC);
                
        PositionBook newBook = new PositionBook(
                state.accountId(),
                state.symbol(),
                newNetQuantity,
                newCostBasis,
                state.realizedPnl(),
                BigDecimal.ZERO, 
                newLots
        );
        return withUnrealizedPnl(newBook, execPrice);
    }

    private PositionBook liquidate(PositionBook state, Execution execution, LotMatchingStrategy strategy) {
        BigDecimal execQty = execution.quantity();
        BigDecimal execPrice = execution.price();
        BigDecimal commission = execution.commission();
        boolean isLong = state.netQuantity().compareTo(BigDecimal.ZERO) > 0;
        
        List<TaxLot> remainingLots = new ArrayList<>(state.openLots());
        
        if (strategy == LotMatchingStrategy.WAC && remainingLots.size() > 1) {
             BigDecimal totalQty = BigDecimal.ZERO;
             BigDecimal totalCost = BigDecimal.ZERO;
             Instant latestTs = Instant.MIN;
             for (TaxLot lot : remainingLots) {
                 totalQty = totalQty.add(lot.quantity(), MC);
                 totalCost = totalCost.add(lot.quantity().multiply(lot.price(), MC), MC);
                 if (lot.timestamp().isAfter(latestTs)) latestTs = lot.timestamp();
             }
             remainingLots.clear();
             remainingLots.add(new TaxLot("POOL", totalQty, totalCost.divide(totalQty, MC), latestTs));
        }

        BigDecimal realizedPnlDelta = BigDecimal.ZERO;
        BigDecimal remainingExecQty = execQty;
        
        while (remainingExecQty.compareTo(BigDecimal.ZERO) > 0 && !remainingLots.isEmpty()) {
            TaxLot oldestLot = remainingLots.get(0);
            if (oldestLot.quantity().compareTo(remainingExecQty) <= 0) {
                BigDecimal qtyToConsume = oldestLot.quantity();
                remainingLots.remove(0);
                
                BigDecimal lotProceeds;
                BigDecimal lotCost;
                if (isLong) {
                    lotProceeds = qtyToConsume.multiply(execPrice, MC);
                    lotCost = qtyToConsume.multiply(oldestLot.price(), MC);
                } else {
                    lotProceeds = qtyToConsume.multiply(oldestLot.price(), MC);
                    lotCost = qtyToConsume.multiply(execPrice, MC);
                }
                realizedPnlDelta = realizedPnlDelta.add(lotProceeds.subtract(lotCost, MC), MC);
                
                remainingExecQty = remainingExecQty.subtract(qtyToConsume, MC);
            } else {
                BigDecimal qtyToConsume = remainingExecQty;
                TaxLot updatedLot = oldestLot.withQuantity(oldestLot.quantity().subtract(qtyToConsume, MC));
                remainingLots.set(0, updatedLot);
                
                BigDecimal lotProceeds;
                BigDecimal lotCost;
                if (isLong) {
                    lotProceeds = qtyToConsume.multiply(execPrice, MC);
                    lotCost = qtyToConsume.multiply(oldestLot.price(), MC);
                } else {
                    lotProceeds = qtyToConsume.multiply(oldestLot.price(), MC);
                    lotCost = qtyToConsume.multiply(execPrice, MC);
                }
                realizedPnlDelta = realizedPnlDelta.add(lotProceeds.subtract(lotCost, MC), MC);
                
                remainingExecQty = BigDecimal.ZERO;
            }
        }

        realizedPnlDelta = realizedPnlDelta.subtract(commission, MC);
        
        BigDecimal newNetQuantity = execution.side() == Side.BUY 
                ? state.netQuantity().add(execQty, MC) 
                : state.netQuantity().subtract(execQty, MC);
                
        BigDecimal newCostBasis = calculateWac(remainingLots);
        
        PositionBook newBook = new PositionBook(
                state.accountId(),
                state.symbol(),
                newNetQuantity,
                newCostBasis,
                state.realizedPnl().add(realizedPnlDelta, MC),
                BigDecimal.ZERO,
                remainingLots
        );
        return withUnrealizedPnl(newBook, execPrice);
    }
    
    private BigDecimal calculateWac(List<TaxLot> lots) {
        if (lots.isEmpty()) return BigDecimal.ZERO;
        BigDecimal totalCost = BigDecimal.ZERO;
        BigDecimal totalQty = BigDecimal.ZERO;
        for (TaxLot lot : lots) {
            totalCost = totalCost.add(lot.quantity().multiply(lot.price(), MC), MC);
            totalQty = totalQty.add(lot.quantity(), MC);
        }
        return totalCost.divide(totalQty, MC);
    }
    
    private PositionBook withUnrealizedPnl(PositionBook book, BigDecimal currentPrice) {
        if (book.netQuantity().compareTo(BigDecimal.ZERO) == 0) {
            return new PositionBook(book.accountId(), book.symbol(), book.netQuantity(), book.costBasis(), book.realizedPnl(), BigDecimal.ZERO, book.openLots());
        }
        BigDecimal unrealizedPnl;
        if (book.netQuantity().compareTo(BigDecimal.ZERO) > 0) {
            unrealizedPnl = currentPrice.subtract(book.costBasis(), MC).multiply(book.netQuantity(), MC);
        } else {
            unrealizedPnl = book.costBasis().subtract(currentPrice, MC).multiply(book.netQuantity().abs(), MC);
        }
        return new PositionBook(book.accountId(), book.symbol(), book.netQuantity(), book.costBasis(), book.realizedPnl(), unrealizedPnl, book.openLots());
    }

    public PositionBook cancelLot(PositionBook state, String originalExecutionId) {
        List<TaxLot> remainingLots = new ArrayList<>(state.openLots());
        boolean removed = remainingLots.removeIf(lot -> lot.id().equals(originalExecutionId));
        
        if (!removed) {
            throw new IllegalArgumentException("Original execution ID not found in open lots: " + originalExecutionId);
        }

        BigDecimal currentPrice = inferCurrentPrice(state);
        BigDecimal newCostBasis = calculateWac(remainingLots);
        
        BigDecimal totalRemainingQty = BigDecimal.ZERO;
        for (TaxLot lot : remainingLots) {
            totalRemainingQty = totalRemainingQty.add(lot.quantity(), MC);
        }
        
        BigDecimal newNetQuantity = state.netQuantity().compareTo(BigDecimal.ZERO) >= 0 
            ? totalRemainingQty 
            : totalRemainingQty.negate();

        PositionBook newBook = new PositionBook(
                state.accountId(),
                state.symbol(),
                newNetQuantity,
                newCostBasis,
                state.realizedPnl(),
                BigDecimal.ZERO, 
                remainingLots
        );
        return withUnrealizedPnl(newBook, currentPrice);
    }

    private BigDecimal inferCurrentPrice(PositionBook book) {
        if (book.netQuantity().compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        if (book.netQuantity().compareTo(BigDecimal.ZERO) > 0) {
            // UnrlPnL = (Price - CB) * Qty => Price = (UnrlPnL / Qty) + CB
            return book.unrealizedPnl().divide(book.netQuantity(), MC).add(book.costBasis(), MC);
        } else {
            // UnrlPnL = (CB - Price) * abs(Qty) => Price = CB - (UnrlPnL / abs(Qty))
            return book.costBasis().subtract(book.unrealizedPnl().divide(book.netQuantity().abs(), MC), MC);
        }
    }
}
