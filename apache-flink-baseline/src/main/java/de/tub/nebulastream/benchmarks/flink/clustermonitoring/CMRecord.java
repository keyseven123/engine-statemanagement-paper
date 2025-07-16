package de.tub.nebulastream.benchmarks.flink.clustermonitoring;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.io.Serializable;

@JsonPropertyOrder({ "creationTS", "jobId", "taskId", "machineId", "eventType", "userId", "category", "priority", "cpu", "ram", "disk", "constraints" })
public class CMRecord implements Serializable {

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

    @Override
    public String toString() {
        return "YourClassName{" +
               "creationTS=" + creationTS +
               ", jobId=" + jobId +
               ", taskId=" + taskId +
               ", machineId=" + machineId +
               ", eventType=" + eventType +
               ", userId=" + userId +
               ", category=" + category +
               ", priority=" + priority +
               ", cpu=" + cpu +
               ", ram=" + ram +
               ", disk=" + disk +
               ", constraints=" + constraints +
               '}';
    }


    public static final int RECORD_SIZE_IN_BYTE = 54;

    public static CsvSchema schema = CsvSchema.builder()
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


    public CMRecord() {}
}



