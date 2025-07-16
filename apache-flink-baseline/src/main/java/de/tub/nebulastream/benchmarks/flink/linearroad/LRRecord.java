package de.tub.nebulastream.benchmarks.flink.linearroad;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.io.Serializable;

@JsonPropertyOrder({ "creationTS", "vehicle", "speed", "highway", "lane", "direction", "position" })
public class LRRecord implements Serializable {
    public long creationTS;
    public int vehicle;
    public float speed;
    public short highway;
    public short lane;
    public short direction;
    public int position;

    public static final int RECORD_SIZE_IN_BYTE = 22;

    @Override
    public String toString() {
        return "LRRecord{" +
                "creationTS=" + creationTS +
                ", vehicle=" + vehicle +
                ", speed=" + speed +
                ", highway=" + highway +
                ", lane=" + lane +
                ", direction=" + direction +
                ", position=" + position +
                '}';
    }

    public static CsvSchema schema = CsvSchema.builder()
                        .addColumn("creationTS", CsvSchema.ColumnType.NUMBER)
                        .addColumn("vehicle", CsvSchema.ColumnType.NUMBER)
                        .addColumn("speed", CsvSchema.ColumnType.NUMBER)
                        .addColumn("highway", CsvSchema.ColumnType.NUMBER)
                        .addColumn("lane", CsvSchema.ColumnType.NUMBER)
                        .addColumn("direction", CsvSchema.ColumnType.NUMBER)
                        .addColumn("position", CsvSchema.ColumnType.NUMBER)
                        .build();

    public LRRecord() {}
}
