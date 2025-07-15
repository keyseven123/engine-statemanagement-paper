package de.tub.nebulastream.benchmarks.flink.clustermonitoring;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@JsonPropertyOrder({ "creationTS", "jobId", "taskId", "machineId", "eventType", "userId", "category", "priority", "cpu", "ram", "disk", "constraints" })
public class CMRecord {

    public long creationTS;
    public long jobId;
    public long taskId;
    public long machineId;
    public short eventType;
    public short userId;
    public short category;
    public short priority;
    public float cpu;
    public float ram;
    public float disk;
    public boolean constraints;

    public static final int RECORD_SIZE_IN_BYTE = 54;


    public CMRecord() {
    }

    public static List<CMRecord> loadFromCsv(String filePath, long numOfRecords) throws Exception {
        CsvMapper csvMapper = new CsvMapper();

        CsvSchema schema = CsvSchema.builder()
                .addColumn("creationTS", CsvSchema.ColumnType.NUMBER)
                .addColumn("jobId", CsvSchema.ColumnType.NUMBER)
                .addColumn("taskId", CsvSchema.ColumnType.NUMBER)
                .addColumn("machineId", CsvSchema.ColumnType.NUMBER)
                .addColumn("eventType", CsvSchema.ColumnType.NUMBER)
                .addColumn("userId", CsvSchema.ColumnType.NUMBER)
                .addColumn("category", CsvSchema.ColumnType.NUMBER)
                .addColumn("priority", CsvSchema.ColumnType.NUMBER)
                .addColumn("cpu", CsvSchema.ColumnType.NUMBER)
                .addColumn("ram", CsvSchema.ColumnType.NUMBER)
                .addColumn("disk", CsvSchema.ColumnType.NUMBER)
                .addColumn("constraints", CsvSchema.ColumnType.BOOLEAN)
                .build();

        MappingIterator<CMRecord> mappingIterator = csvMapper
               .readerFor(CMRecord.class)
               .with(schema)
               .readValues(new File(filePath));

        return StreamSupport.stream(((Iterable<CMRecord>) () -> mappingIterator).spliterator(), false)
               .limit(numOfRecords)
               .collect(Collectors.toList());

    }
}



