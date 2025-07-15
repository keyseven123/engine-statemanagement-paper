package de.tub.nebulastream.benchmarks.flink.manufacturingequipment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@JsonPropertyOrder({ "creationTS", "messageIndex", "mf01", "mf02", "mf03", "pc13", "pc14", "pc15", "pc25", "pc26", "pc27", "res", "bm05", "bm06" })
public class MERecord {
    public long creationTS;
    public long messageIndex;
    public short mf01;
    public short mf02;
    public short mf03;
    public short pc13;
    public short pc14;
    public short pc15;
    public short pc25;
    public short pc26;
    public short pc27;
    public short res;
    public short bm05;
    public short bm06;

    public static final int RECORD_SIZE_IN_BYTE = 40;

    public MERecord() {
    }
    
    public static List<MERecord> loadFromCsv(String filePath, long numOfRecords) throws Exception {
            CsvMapper csvMapper = new CsvMapper();
    
            CsvSchema schema = CsvSchema.builder()
                    .addColumn("creationTS", CsvSchema.ColumnType.NUMBER)
                    .addColumn("messageIndex", CsvSchema.ColumnType.NUMBER)
                    .addColumn("mf01", CsvSchema.ColumnType.NUMBER)
                    .addColumn("mf02", CsvSchema.ColumnType.NUMBER)
                    .addColumn("mf03", CsvSchema.ColumnType.NUMBER)
                    .addColumn("pc13", CsvSchema.ColumnType.NUMBER)
                    .addColumn("pc14", CsvSchema.ColumnType.NUMBER)
                    .addColumn("pc15", CsvSchema.ColumnType.NUMBER)
                    .addColumn("pc25", CsvSchema.ColumnType.NUMBER)
                    .addColumn("pc26", CsvSchema.ColumnType.NUMBER)
                    .addColumn("pc27", CsvSchema.ColumnType.NUMBER)
                    .addColumn("res", CsvSchema.ColumnType.NUMBER)
                    .addColumn("bm05", CsvSchema.ColumnType.NUMBER)
                    .addColumn("bm06", CsvSchema.ColumnType.NUMBER)
                    .setUseHeader(true) // As the first line contains floats for some reason
                    .build();
    
            MappingIterator<MERecord> mappingIterator = csvMapper
                   .readerFor(MERecord.class)
                   .with(schema)
                   .readValues(new File(filePath));
    
            return StreamSupport.stream(((Iterable<MERecord>) () -> mappingIterator).spliterator(), false)
                   .limit(numOfRecords)
                   .collect(Collectors.toList());
    
        }
}



