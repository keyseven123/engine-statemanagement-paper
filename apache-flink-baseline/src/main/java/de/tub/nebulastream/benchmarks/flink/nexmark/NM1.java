package de.tub.nebulastream.benchmarks.flink.nexmark;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.nexmark.NMBidRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import de.tub.nebulastream.benchmarks.flink.util.MemorySource;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.util.function.Function;
import org.apache.flink.util.function.SerializableFunction;
import java.io.File;
import java.util.List;

public class NM1 {

    private static final Logger LOG = LoggerFactory.getLogger(NM1.class);

    /**
     * SELECT itemid, DOLTOEUR(price),
     * bidderId, bidTime
     * FROM bid;
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
        final int parallelism = params.getInt("parallelism", 1);
        final long numOfRecords = params.getLong("numOfRecords", 1_000_000);
        final int maxRuntimeInSeconds = params.getInt("maxRuntime", 10);

        LOG.info("Arguments: {}", params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

        MemorySource<NMBidRecord> source = new MemorySource<NMBidRecord>("/tmp/data/bid_more_data_6GB.csv", numOfRecords, NMBidRecord.class, NMBidRecord.schema);
        DataStream<NMBidRecord> sourceStream = env
                    .fromSource(source, WatermarkStrategy.noWatermarks(), "Bid_Source")
                    .returns(TypeExtractor.getForClass(NMBidRecord.class))
                    .setParallelism(1);


        sourceStream
            .flatMap(new ThroughputLogger<NMBidRecord>(500))
            .map(new MapFunction<NMBidRecord, Tuple4<Integer, Double, Integer, Integer>>() {
                    @Override
                public Tuple4<Integer, Double, Integer, Integer> map(NMBidRecord record) throws Exception {
                    return new Tuple4<>(record.auctionId, (record.price * 89 / 100), record.bidder, record.auctionId);
                }
            }).project(0, 2)
            .sinkTo(new DiscardingSink<Tuple>() {
            });

        // Sleep maxRuntimeInSeconds seconds and then cancel
        JobClient jobClient = env.executeAsync("NM1");
        LOG.info("Started flink job");
        Thread.sleep(maxRuntimeInSeconds * 1000);
        jobClient.cancel().thenRun(() ->
            LOG.info("Job cancelled after {} seconds.", maxRuntimeInSeconds)
        );
    }
}
