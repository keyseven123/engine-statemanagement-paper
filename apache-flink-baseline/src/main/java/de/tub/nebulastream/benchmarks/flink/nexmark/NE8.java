package de.tub.nebulastream.benchmarks.flink.nexmark;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.nexmark.NEPersonRecord;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import java.time.Duration;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import java.io.File;
import java.util.List;

public class NE8 {

    private static final Logger LOG = LoggerFactory.getLogger(NE8.class);

    /**
     * SELECT Rstream(P.id, P.name, A.reserve)
     * FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
     * WHERE P.id = A.seller;
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
        final int parallelism = params.getInt("parallelism", 1);
        final long numOfRecords = params.getLong("numOfRecords", 1_000_000);


        LOG.info("Arguments: {}", params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

        // Auctions stream
        WatermarkStrategy<NEAuctionRecord> strategyAuction = WatermarkStrategy
                .<NEAuctionRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                .withTimestampAssigner((event, timestamp) -> event.timestamp);
        List<NEAuctionRecord> auctionRecords = NEAuctionRecord.loadFromCsv("/tmp/data/auction_more_data_707MB.csv", numOfRecords);
        DataStream<NEAuctionRecord> auctions = env
             .fromCollection(auctionRecords)
             .assignTimestampsAndWatermarks(strategyAuction)
             .name("NE8_Auctions");

        // Persons stream
        WatermarkStrategy<NEPersonRecord> strategyPerson = WatermarkStrategy
                .<NEPersonRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                .withTimestampAssigner((event, timestamp) -> event.timestamp);
             List<NEPersonRecord> personRecords = NEPersonRecord.loadFromCsv("/tmp/data/person_more_data_840MB.csv", numOfRecords);
             DataStream<NEPersonRecord> persons = env
                  .fromCollection(personRecords)
                  .assignTimestampsAndWatermarks(strategyPerson)
                  .name("NE8_Persons");

        auctions.flatMap(new ThroughputLogger<NEAuctionRecord>(NEAuctionRecord.RECORD_SIZE_IN_BYTE, 10_000));
        persons.flatMap(new ThroughputLogger<NEPersonRecord>(NEPersonRecord.RECORD_SIZE_IN_BYTE, 10_000));


        auctions.join(persons).where(new KeySelector<NEAuctionRecord, Integer>() {
                    @Override
                    public Integer getKey(NEAuctionRecord value) throws Exception {
                        return value.seller;
                    }
                }).equalTo(new KeySelector<NEPersonRecord, Integer>() {
                    @Override
                    public Integer getKey(NEPersonRecord value) throws Exception {
                        return value.id;
                    }
                }).window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))).apply(new FlatJoinFunction<NEAuctionRecord, NEPersonRecord, Tuple2<Integer, Integer>>() {
                    @Override
                    public void join(NEAuctionRecord first, NEPersonRecord second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(first.seller, second.id));
                    }
                })
                .sinkTo(new DiscardingSink<Tuple2<Integer, Integer>>() {
                });


        env.execute("NE8");

    }
}
