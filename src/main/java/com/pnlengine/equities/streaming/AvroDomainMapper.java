package com.pnlengine.equities.streaming;

import com.pnlengine.equities.domain.Execution;
import com.pnlengine.equities.domain.PositionBook;
import com.pnlengine.equities.domain.Side;
import com.pnlengine.equities.domain.TaxLot;
import com.pnlengine.equities.streaming.avro.AvroExecution;
import com.pnlengine.equities.streaming.avro.AvroPositionBook;
import com.pnlengine.equities.streaming.avro.AvroTaxLot;

import java.util.List;
import java.util.stream.Collectors;

public class AvroDomainMapper {

    public static Execution toDomain(AvroExecution avro) {
        return new Execution(
                avro.getId(),
                avro.getAccountId(),
                avro.getSymbol(),
                Side.valueOf(avro.getSide()),
                avro.getQuantity(),
                avro.getPrice(),
                avro.getCommission(),
                avro.getTimestamp()
        );
    }

    public static PositionBook toDomain(AvroPositionBook avro) {
        if (avro == null) {
            return null;
        }
        
        List<TaxLot> lots = avro.getOpenLots().stream()
                .map(AvroDomainMapper::toDomain)
                .collect(Collectors.toList());

        return new PositionBook(
                avro.getAccountId(),
                avro.getSymbol(),
                avro.getNetQuantity(),
                avro.getCostBasis(),
                avro.getRealizedPnl(),
                avro.getUnrealizedPnl(),
                lots
        );
    }

    public static TaxLot toDomain(AvroTaxLot avro) {
        return new TaxLot(
                avro.getId(),
                avro.getQuantity(),
                avro.getPrice(),
                avro.getTimestamp()
        );
    }

    public static AvroPositionBook toAvro(PositionBook domain) {
        if (domain == null) return null;
        
        List<AvroTaxLot> avroLots = domain.openLots().stream()
                .map(AvroDomainMapper::toAvro)
                .collect(Collectors.toList());

        return AvroPositionBook.newBuilder()
                .setAccountId(domain.accountId())
                .setSymbol(domain.symbol())
                .setNetQuantity(domain.netQuantity())
                .setCostBasis(domain.costBasis())
                .setRealizedPnl(domain.realizedPnl())
                .setUnrealizedPnl(domain.unrealizedPnl())
                .setOpenLots(avroLots)
                .build();
    }

    public static AvroTaxLot toAvro(TaxLot domain) {
        return AvroTaxLot.newBuilder()
                .setId(domain.id())
                .setQuantity(domain.quantity())
                .setPrice(domain.price())
                .setTimestamp(domain.timestamp())
                .build();
    }
}
