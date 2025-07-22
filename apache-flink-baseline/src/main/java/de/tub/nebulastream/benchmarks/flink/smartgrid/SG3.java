package de.tub.nebulastream.benchmarks.flink.smartgrid;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.smartgrid.SGRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import de.tub.nebulastream.benchmarks.flink.util.MemorySource;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import java.io.File;
import java.util.List;

public class SG3 {

    private static final Logger LOG = LoggerFactory.getLogger(SG3.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
        final int parallelism = params.getInt("parallelism", 1);
        final int maxParallelism = params.getInt("maxParallelism", 16);
        final long numOfRecords = params.getLong("numOfRecords", 1_000_000);
        final int maxRuntimeInSeconds = params.getInt("maxRuntime", 10);
        final String basePathForDataFiles = params.get("basePathForDataFiles", "/tmp/data");

        LOG.info("Arguments: {}", params);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

        MemorySource<SGRecord> source = new MemorySource<SGRecord>(basePathForDataFiles + "/smartgrid-data_6GB.csv", numOfRecords, SGRecord.class, SGRecord.schema);
        WatermarkStrategy<SGRecord> strategy = WatermarkStrategy
             .<SGRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
             .withTimestampAssigner((event, timestamp) -> event.creationTS / 1000);
        DataStream<SGRecord> sourceStream = env
                    .fromSource(source, strategy, "SG_Source")
                    .returns(TypeExtractor.getForClass(SGRecord.class))
                    .setParallelism(1);


        sourceStream
            .flatMap(new ThroughputLogger<SGRecord>(500))
            .keyBy(new KeySelector<SGRecord, Tuple3<Short, Short, Short>>() {
                    @Override
                    public Tuple3<Short, Short, Short> getKey(SGRecord sgRecord) throws Exception {
                        return new Tuple3<>(sgRecord.plug, sgRecord.household, sgRecord.house);
                    }
                })
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(128), Duration.ofSeconds(1)))
                .aggregate(new AggregateFunction<SGRecord, Tuple2<Long, Double>, Double>() {
                    @Override
                    public Tuple2<Long, Double> createAccumulator() {
                        return new Tuple2<>(0L, 0.0);
                    }

                    @Override
                    public Tuple2<Long, Double> add(SGRecord value, Tuple2<Long, Double> accumulator) {
                        return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + value.value);
                    }

                    @Override
                    public Double getResult(Tuple2<Long, Double> accumulator) {
                        return accumulator.f1 / accumulator.f0;
                    }

                    @Override
                    public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                })
                .setMaxParallelism(maxParallelism)
                .name("WindowOperator")
                .sinkTo(new DiscardingSink<Double>() {
                });

        // Sleep maxRuntimeInSeconds seconds and then cancel
        JobClient jobClient = env.executeAsync("SG3");
        LOG.info("Started flink job");
        Thread.sleep(maxRuntimeInSeconds * 1000);
        jobClient.cancel().thenRun(() ->
            LOG.info("Job cancelled after {} seconds.", maxRuntimeInSeconds)
        );

    }
}
