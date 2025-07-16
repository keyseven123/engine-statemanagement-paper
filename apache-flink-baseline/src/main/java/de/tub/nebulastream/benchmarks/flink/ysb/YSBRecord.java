package de.tub.nebulastream.benchmarks.flink.ysb;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.io.Serializable;

@JsonPropertyOrder({ "user_id",  "page_id",  "campaign_id",  "ad_id",  "ad_type",  "event_type",  "event_time",  "ip_address" })
public class YSBRecord implements Serializable {
    public String user_id;
    public String page_id;
    public String campaign_id;
    public String ad_id;
    public String ad_type;
    public String event_type;
    public long event_time;
    public String ip_address;
    static int RECORD_SIZE_IN_BYTE = 80;

    @Override
    public String toString() {
        return "YourClassName{" +
                "user_id='" + user_id + '\'' +
                ", page_id='" + page_id + '\'' +
                ", campaign_id='" + campaign_id + '\'' +
                ", ad_id='" + ad_id + '\'' +
                ", ad_type='" + ad_type + '\'' +
                ", event_type='" + event_type + '\'' +
                ", event_time=" + event_time +
                ", ip_address='" + ip_address + '\'' +
                '}';
    }

    public static CsvSchema schema = CsvSchema.builder()
                                  .addColumn("user_id", CsvSchema.ColumnType.STRING)
                                  .addColumn("page_id", CsvSchema.ColumnType.STRING)
                                  .addColumn("campaign_id", CsvSchema.ColumnType.STRING)
                                  .addColumn("ad_id", CsvSchema.ColumnType.STRING)
                                  .addColumn("ad_type", CsvSchema.ColumnType.STRING)
                                  .addColumn("event_type", CsvSchema.ColumnType.STRING)
                                  .addColumn("event_time", CsvSchema.ColumnType.NUMBER)
                                  .addColumn("ip_address", CsvSchema.ColumnType.STRING)
                                  .build();

    public YSBRecord() {}
}



