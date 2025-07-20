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

@JsonPropertyOrder({"id", "name", "email_address", "credit_card", "city", "state", "timestamp", "extra"})
public class NMPersonRecord implements Serializable {
    public int id;
    public String name;
    public String email_address;
    public String credit_card;
    public String city;
    public String state;
    public long timestamp;
    public String extra;
    static int RECORD_SIZE_IN_BYTE = 40;
    public static CsvSchema schema = CsvSchema.builder()
                                      .addColumn("id", CsvSchema.ColumnType.NUMBER)
                                      .addColumn("name", CsvSchema.ColumnType.STRING)
                                      .addColumn("email_address", CsvSchema.ColumnType.STRING)
                                      .addColumn("credit_card", CsvSchema.ColumnType.STRING)
                                      .addColumn("city", CsvSchema.ColumnType.STRING)
                                      .addColumn("state", CsvSchema.ColumnType.STRING)
                                      .addColumn("timestamp", CsvSchema.ColumnType.NUMBER)
                                      .addColumn("extra", CsvSchema.ColumnType.STRING)
                                      .build();

    public NMPersonRecord() {}
}
