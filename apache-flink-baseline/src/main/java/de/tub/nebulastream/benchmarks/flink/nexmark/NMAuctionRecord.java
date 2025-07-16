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

@JsonPropertyOrder({"timestamp", "id", "initialBid", "reserve", "expires", "seller", "category"})
public class NMAuctionRecord implements Serializable {
    public long timestamp;
    public int id;
    public double initialBid;
    public int reserve;
    public long expires;
    public int seller;
    public int category;

    public static final int RECORD_SIZE_IN_BYTE = 44;
    public static CsvSchema schema = CsvSchema.builder()
                                  .addColumn("timestamp", CsvSchema.ColumnType.NUMBER)
                                  .addColumn("id", CsvSchema.ColumnType.NUMBER)
                                  .addColumn("initialBid", CsvSchema.ColumnType.NUMBER)
                                  .addColumn("reserve", CsvSchema.ColumnType.NUMBER)
                                  .addColumn("expires", CsvSchema.ColumnType.NUMBER)
                                  .addColumn("seller", CsvSchema.ColumnType.NUMBER)
                                  .addColumn("category", CsvSchema.ColumnType.NUMBER)
                                  .build();


    public NMAuctionRecord() {}
}
