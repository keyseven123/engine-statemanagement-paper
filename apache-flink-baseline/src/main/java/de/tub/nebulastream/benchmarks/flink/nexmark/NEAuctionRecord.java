package de.tub.nebulastream.benchmarks.flink.nexmark;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class NEAuctionRecord {
    public long timestamp;
    public int id;
    public double initialBid;
    public int reserve;
    public long expires;
    public int seller;
    public int category;

    public static final int RECORD_SIZE_IN_BYTE = 44;

    public NEAuctionRecord() {}

    public static List<NEAuctionRecord> loadFromCsv(String filePath, long numOfRecords) throws Exception {
        CsvMapper csvMapper = new CsvMapper();

        CsvSchema schema = CsvSchema.builder()
                .addColumn("timestamp", CsvSchema.ColumnType.NUMBER)
                .addColumn("id", CsvSchema.ColumnType.NUMBER)
                .addColumn("initialBid", CsvSchema.ColumnType.NUMBER)
                .addColumn("reserve", CsvSchema.ColumnType.NUMBER)
                .addColumn("expires", CsvSchema.ColumnType.NUMBER)
                .addColumn("seller", CsvSchema.ColumnType.NUMBER)
                .addColumn("category", CsvSchema.ColumnType.NUMBER)
                .build();

       MappingIterator<NEAuctionRecord> mappingIterator = csvMapper
               .readerFor(NEAuctionRecord.class)
               .with(schema)
               .readValues(new File(filePath));

       return StreamSupport.stream(((Iterable<NEAuctionRecord>) () -> mappingIterator).spliterator(), false)
               .limit(numOfRecords)
               .collect(Collectors.toList());
    }
}
