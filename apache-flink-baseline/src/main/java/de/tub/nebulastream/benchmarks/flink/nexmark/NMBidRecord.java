package de.tub.nebulastream.benchmarks.flink.nexmark;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.io.Serializable;

@JsonPropertyOrder({"timestamp", "auctionId", "bidder", "price"})
public class NMBidRecord implements Serializable {
    public long timestamp;
    public int auctionId;
    public int bidder;
    public double price;

    public static final int RECORD_SIZE_IN_BYTE = 32;
    public static CsvSchema schema = CsvSchema.builder()
                                 .addColumn("timestamp", CsvSchema.ColumnType.NUMBER)
                                 .addColumn("auctionId", CsvSchema.ColumnType.NUMBER)
                                 .addColumn("bidder", CsvSchema.ColumnType.NUMBER)
                                 .addColumn("price", CsvSchema.ColumnType.NUMBER)
                                 .build();

    public NMBidRecord() {}
}

