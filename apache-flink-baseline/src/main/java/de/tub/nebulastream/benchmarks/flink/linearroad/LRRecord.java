package de.tub.nebulastream.benchmarks.flink.linearroad;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@JsonPropertyOrder({ "creationTS", "vehicle", "speed", "highway", "lane", "direction", "position" })
public class LRRecord {
    public long creationTS;
    public short vehicle;
    public float speed;
    public short highway;
    public short lane;
    public short direction;
    public int position;

    public static final int RECORD_SIZE_IN_BYTE = 22;


    public LRRecord() {
    }

    public static List<LRRecord> loadFromCsv(String filePath, long numOfRecords) throws Exception {
            CsvMapper csvMapper = new CsvMapper();

            CsvSchema schema = CsvSchema.builder()
                    .addColumn("creationTS", CsvSchema.ColumnType.NUMBER)
                    .addColumn("vehicle", CsvSchema.ColumnType.NUMBER)
                    .addColumn("speed", CsvSchema.ColumnType.NUMBER)
                    .addColumn("highway", CsvSchema.ColumnType.NUMBER)
                    .addColumn("lane", CsvSchema.ColumnType.NUMBER)
                    .addColumn("direction", CsvSchema.ColumnType.NUMBER)
                    .addColumn("position", CsvSchema.ColumnType.NUMBER)
                    .build();

            MappingIterator<LRRecord> mappingIterator = csvMapper
                   .readerFor(LRRecord.class)
                   .with(schema)
                   .readValues(new File(filePath));

            return StreamSupport.stream(((Iterable<LRRecord>) () -> mappingIterator).spliterator(), false)
                   .limit(numOfRecords)
                   .collect(Collectors.toList());

        }
}
