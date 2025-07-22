package de.tub.nebulastream.benchmarks.flink.ysb;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import de.tub.nebulastream.benchmarks.flink.utils.AnalyzeTool;
import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import de.tub.nebulastream.benchmarks.flink.util.MemorySource;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.time.Duration;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import java.io.File;
import java.util.List;

public class YSB10k {

    private static final Logger LOG = LoggerFactory.getLogger(YSB10k.class);

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

        MemorySource<YSBRecord> source = new MemorySource<YSBRecord>(basePathForDataFiles + "/ysb10k_more_data_3GB.csv", numOfRecords, YSBRecord.class, YSBRecord.schema);
        WatermarkStrategy<YSBRecord> strategy = WatermarkStrategy
             .<YSBRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
             .withTimestampAssigner((event, timestamp) -> event.event_time / 1000);
       DataStream<YSBRecord> sourceStream = env
                    .fromSource(source, strategy, "YSB10k_Source")
                    .returns(TypeExtractor.getForClass(YSBRecord.class))
                    .setParallelism(1);

        sourceStream
            .flatMap(new ThroughputLogger<YSBRecord>(500))
            .filter(new FilterFunction<YSBRecord>() {
                @Override
                public boolean filter(YSBRecord value) throws Exception {
                    return "view".equals(value.event_type);
                }
            })
            .keyBy((KeySelector<YSBRecord, String>) r -> r.campaign_id)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
            .aggregate(new WindowingLogic())
            .sinkTo(new DiscardingSink<Long>() {
            });


        // Sleep maxRuntimeInSeconds seconds and then cancel
        JobClient jobClient = env.executeAsync("YSB10k");
        LOG.info("Started flink job");
        Thread.sleep(maxRuntimeInSeconds * 1000);
        jobClient.cancel().thenRun(() ->
            LOG.info("Job cancelled after {} seconds.", maxRuntimeInSeconds)
        );

    }


    private static class WindowingLogic implements AggregateFunction<YSBRecord, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(YSBRecord value, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
