package de.tub.nebulastream.benchmarks.flink.nexmark;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.nexmark.NMPersonRecord;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import de.tub.nebulastream.benchmarks.flink.util.MemorySource;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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

public class NM8 {

    private static final Logger LOG = LoggerFactory.getLogger(NM8.class);

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
        final int maxRuntimeInSeconds = params.getInt("maxRuntime", 10);
        final String basePathForDataFiles = params.get("basePathForDataFiles", "/tmp/data");


        LOG.info("Arguments: {}", params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

        // Auctions stream
        WatermarkStrategy<NMAuctionRecord> strategyAuction = WatermarkStrategy
                .<NMAuctionRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                .withTimestampAssigner((event, timestamp) -> event.timestamp / 1000);
        MemorySource<NMAuctionRecord> auctionSource = new MemorySource<NMAuctionRecord>(basePathForDataFiles + "/auction_more_data_707MB.csv", numOfRecords, NMAuctionRecord.class, NMAuctionRecord.schema);
        DataStream<NMAuctionRecord> sourceStreamAuctions = env
            .fromSource(auctionSource, strategyAuction, "Auction_Source")
            .returns(TypeExtractor.getForClass(NMAuctionRecord.class))
            .setParallelism(1);

        // Persons stream
        WatermarkStrategy<NMPersonRecord> strategyPerson = WatermarkStrategy
                .<NMPersonRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                .withTimestampAssigner((event, timestamp) -> event.timestamp / 1000);
        MemorySource<NMPersonRecord> personSource = new MemorySource<NMPersonRecord>(basePathForDataFiles + "/person_more_data_840MB.csv", numOfRecords, NMPersonRecord.class, NMPersonRecord.schema);
        DataStream<NMPersonRecord> sourceStreamPersons = env
            .fromSource(personSource, strategyPerson, "Person_Source")
            .returns(TypeExtractor.getForClass(NMPersonRecord.class))
            .setParallelism(1);

        sourceStreamAuctions.flatMap(new ThroughputLogger<NMAuctionRecord>(500));
        sourceStreamPersons.flatMap(new ThroughputLogger<NMPersonRecord>(500));


        sourceStreamAuctions.join(sourceStreamPersons).where(new KeySelector<NMAuctionRecord, Integer>() {
                    @Override
                    public Integer getKey(NMAuctionRecord value) throws Exception {
                        return value.seller;
                    }
                }).equalTo(new KeySelector<NMPersonRecord, Integer>() {
                    @Override
                    public Integer getKey(NMPersonRecord value) throws Exception {
                        return value.id;
                    }
                }).window(TumblingEventTimeWindows.of(Duration.ofSeconds(10))).apply(new FlatJoinFunction<NMAuctionRecord, NMPersonRecord, Tuple2<Integer, Integer>>() {
                    @Override
                    public void join(NMAuctionRecord first, NMPersonRecord second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(first.seller, second.id));
                    }
                })
                .sinkTo(new DiscardingSink<Tuple2<Integer, Integer>>() {
                });


        // Sleep maxRuntimeInSeconds seconds and then cancel
        JobClient jobClient = env.executeAsync("NM8");
        LOG.info("Started flink job");
        Thread.sleep(maxRuntimeInSeconds * 1000);
        jobClient.cancel().thenRun(() ->
            LOG.info("Job cancelled after {} seconds.", maxRuntimeInSeconds)
        );

    }
}
