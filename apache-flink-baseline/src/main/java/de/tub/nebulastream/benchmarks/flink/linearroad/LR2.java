package de.tub.nebulastream.benchmarks.flink.linearroad;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.linearroad.LRRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import de.tub.nebulastream.benchmarks.flink.util.MemorySource;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import java.io.File;
import java.util.List;

public class LR2 {

    private static final Logger LOG = LoggerFactory.getLogger(LR2.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
        final int parallelism = params.getInt("parallelism", 1);
        final int numOfRecords = params.getInt("numOfRecords", 1_000_000);
        final int maxRuntimeInSeconds = params.getInt("maxRuntime", 10);
        final String basePathForDataFiles = params.get("basePathForDataFiles", "/tmp/data");

        LOG.info("Arguments: {}", params);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

        MemorySource<LRRecord> source = new MemorySource<LRRecord>(basePathForDataFiles + "/linear_road_benchmark_5GB.csv", numOfRecords, LRRecord.class, LRRecord.schema);
        WatermarkStrategy<LRRecord> strategy = WatermarkStrategy
                .<LRRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                .withTimestampAssigner((event, timestamp) -> event.creationTS / 1000);
        DataStream<LRRecord> sourceStream = env
            .fromSource(source, strategy, "LR_Source")
            .returns(TypeExtractor.getForClass(LRRecord.class))
            .setParallelism(1);


        sourceStream
            .flatMap(new ThroughputLogger<LRRecord>(500))
            .map(value -> {
                    value.position = (int) (value.position / 5280);
                    return value;
                })
                .keyBy(new KeySelector<LRRecord, Tuple4<Integer, Short, Short, Integer>>() {
                    @Override
                    public Tuple4<Integer, Short, Short, Integer> getKey(LRRecord value) throws Exception {
                       return new Tuple4<>(value.vehicle, value.highway, value.direction, value.position);
                    }
                })
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(30), Duration.ofSeconds(1)))
                .aggregate(new AggregateFunction<LRRecord, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(LRRecord value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                })
                .name("WindowOperator")
                .sinkTo(new DiscardingSink<Long>() {
                });

        JobClient jobClient = env.executeAsync("LR2");

        // Sleep maxRuntimeInSeconds seconds and then cancel
        LOG.info("Started flink job");
        Thread.sleep(maxRuntimeInSeconds * 1000);
        jobClient.cancel().thenRun(() ->
            LOG.info("Job cancelled after {} seconds.", maxRuntimeInSeconds)
        );

    }
}
