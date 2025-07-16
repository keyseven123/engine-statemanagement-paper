package de.tub.nebulastream.benchmarks.flink.clustermonitoring;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.clustermonitoring.CMRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import java.io.File;
import java.util.List;

public class CM2 {

    private static final Logger LOG = LoggerFactory.getLogger(CM2.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
        final int parallelism = params.getInt("parallelism", 1);
        final int numOfRecords = params.getInt("numOfRecords", 1_000_000);
        final int maxRuntimeInSeconds = params.getInt("maxRuntime", 10);

        LOG.info("Arguments: {}", params);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

         MemorySource<CMRecord> source = new MemorySource<CMRecord>("/tmp/data/google-cluster-data-original_1G.csv", numOfRecords, CMRecord.class, CMRecord.schema);
         WatermarkStrategy<CMRecord> strategy = WatermarkStrategy
                 .<CMRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                 .withTimestampAssigner((event, timestamp) -> event.creationTS / 1000);
         DataStream<CMRecord> sourceStream = env
                     .fromSource(source, strategy, "CM_Source")
                     .returns(TypeExtractor.getForClass(CMRecord.class))
                     .setParallelism(1);

         sourceStream
             .flatMap(new ThroughputLogger<CMRecord>(500))
             .filter(new FilterFunction<CMRecord>() {
                    @Override
                    public boolean filter(CMRecord cmRecord) throws Exception {
                        return cmRecord.eventType == 3;
                    }
                })
                .keyBy(new KeySelector<CMRecord, Long>() {
                    @Override
                    public Long getKey(CMRecord cmRecord) throws Exception {
                        return cmRecord.jobId;
                    }
                })
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(60), Duration.ofSeconds(1)))
                .aggregate(new AggregateFunction<CMRecord, Double, Double>() {
                    @Override
                    public Double createAccumulator() {
                        return 0.0;
                    }

                    @Override
                    public Double add(CMRecord cmRecord, Double aLong) {
                        return aLong + cmRecord.cpu;
                    }

                    @Override
                    public Double getResult(Double aLong) {
                        return aLong;
                    }

                    @Override
                    public Double merge(Double aLong, Double acc1) {
                        return aLong + acc1;
                    }
                })
                .name("WindowOperator")
                .sinkTo(new DiscardingSink<Double>() {
                });

        // Sleep maxRuntimeInSeconds seconds and then cancel
        JobClient jobClient = env.executeAsync("CM2");
        LOG.info("Started flink job");
        Thread.sleep(maxRuntimeInSeconds * 1000);
        jobClient.cancel().thenRun(() ->
            LOG.info("Job cancelled after {} seconds.", maxRuntimeInSeconds)
        );

    }
}
