package de.tub.nebulastream.benchmarks.flink.smartgrid;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.io.Serializable;

@JsonPropertyOrder({ "creationTS",  "value",  "property",  "plug",  "household",  "house" })
public class SGRecord implements Serializable {

    public long creationTS;
    public float value;
    public short property;
    public short plug;
    public short household;
    public short house;
    static int RECORD_SIZE_IN_BYTE = 20;
    public static CsvSchema schema = CsvSchema.builder()
                                      .addColumn("creationTS", CsvSchema.ColumnType.NUMBER)
                                      .addColumn("value", CsvSchema.ColumnType.NUMBER)
                                      .addColumn("property", CsvSchema.ColumnType.NUMBER)
                                      .addColumn("plug", CsvSchema.ColumnType.NUMBER)
                                      .addColumn("household", CsvSchema.ColumnType.NUMBER)
                                      .addColumn("house", CsvSchema.ColumnType.NUMBER)
                                      .build();


    public SGRecord() {}
}



