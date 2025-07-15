package de.tub.nebulastream.benchmarks.flink.ysb;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@JsonPropertyOrder({ "user_id",  "page_id",  "campaign_id",  "ad_id",  "ad_type",  "event_type",  "event_time",  "ip_address" })
public class YSBRecord {
    public String user_id;
    public String page_id;
    public String campaign_id;
    public String ad_id;
    public String ad_type;
    public String event_type;
    public long event_time;
    public String ip_address;
    static int RECORD_SIZE_IN_BYTE = 80;

    public YSBRecord() {}

    public static List<YSBRecord> loadFromCsv(String filePath, long numOfRecords) throws Exception {
        CsvMapper csvMapper = new CsvMapper();

        CsvSchema schema = CsvSchema.builder()
                .addColumn("user_id", CsvSchema.ColumnType.STRING)
                .addColumn("page_id", CsvSchema.ColumnType.STRING)
                .addColumn("campaign_id", CsvSchema.ColumnType.STRING)
                .addColumn("ad_id", CsvSchema.ColumnType.STRING)
                .addColumn("ad_type", CsvSchema.ColumnType.STRING)
                .addColumn("event_type", CsvSchema.ColumnType.STRING)
                .addColumn("event_time", CsvSchema.ColumnType.NUMBER)
                .addColumn("ip_address", CsvSchema.ColumnType.STRING)
                .build();

        MappingIterator<YSBRecord> mappingIterator = csvMapper
               .readerFor(YSBRecord.class)
               .with(schema)
               .readValues(new File(filePath));

        return StreamSupport.stream(((Iterable<YSBRecord>) () -> mappingIterator).spliterator(), false)
               .limit(numOfRecords)
               .collect(Collectors.toList());
    }

}



