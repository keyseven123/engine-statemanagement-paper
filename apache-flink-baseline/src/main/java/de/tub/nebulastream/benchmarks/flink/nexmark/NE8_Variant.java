package de.tub.nebulastream.benchmarks.flink.nexmark;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.nexmark.NEAuctionRecord;
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

public class NE8_Variant {

    private static final Logger LOG = LoggerFactory.getLogger(NE8_Variant.class);

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

        // Bids stream
        WatermarkStrategy<NEBidRecord> strategyBids = WatermarkStrategy
                .<NEBidRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> event.timestamp);
        List<NEBidRecord> bidRecords = NEBidRecord.loadFromCsv("/tmp/data/bid_more_data_6GB.csv", numOfRecords);
        DataStream<NEBidRecord> bids = env
                .fromCollection(bidRecords)
                .assignTimestampsAndWatermarks(strategyBids)
                .name("NE8_Variant_Bids");

        // Auctions stream
        WatermarkStrategy<NEAuctionRecord> strategyAuction = WatermarkStrategy
                .<NEAuctionRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                .withTimestampAssigner((event, timestamp) -> event.timestamp);
        List<NEAuctionRecord> auctionRecords = NEAuctionRecord.loadFromCsv("/tmp/data/auction_more_data_707MB.csv", numOfRecords);
        DataStream<NEAuctionRecord> auctions = env
             .fromCollection(auctionRecords)
             .assignTimestampsAndWatermarks(strategyAuction)
             .name("NE8_Auctions");

        bids.flatMap(new ThroughputLogger<NEBidRecord>(NEBidRecord.RECORD_SIZE_IN_BYTE, 10_000));
        auctions.flatMap(new ThroughputLogger<NEAuctionRecord>(NEAuctionRecord.RECORD_SIZE_IN_BYTE, 10_000));


        bids.join(auctions).where(new KeySelector<NEBidRecord, Integer>() {
                    @Override
                    public Integer getKey(NEBidRecord value) throws Exception {
                        return value.auctionId;
                    }
                }).equalTo(new KeySelector<NEAuctionRecord, Integer>() {
                    @Override
                    public Integer getKey(NEAuctionRecord value) throws Exception {
                        return value.id;
                    }
                }).window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))).apply(new FlatJoinFunction<NEBidRecord, NEAuctionRecord, Tuple2<Integer, Integer>>() {
                    @Override
                    public void join(NEBidRecord first, NEAuctionRecord second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(first.auctionId, second.id));
                    }
                })
                .sinkTo(new DiscardingSink<Tuple2<Integer, Integer>>() {
                });


        env.execute("NE8_Variant");

    }
}
