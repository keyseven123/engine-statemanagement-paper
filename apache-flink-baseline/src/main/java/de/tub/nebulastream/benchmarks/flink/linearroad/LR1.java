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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
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

public class LR1 {

    private static final Logger LOG = LoggerFactory.getLogger(LR1.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
        final int parallelism = params.getInt("parallelism", 1);
        final int numOfRecords = params.getInt("numOfRecords", 1_000_000);

        LOG.info("Arguments: {}", params);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

        List<LRRecord> records = LRRecord.loadFromCsv("/tmp/data/linear_road_benchmark_560K.csv", numOfRecords);
        WatermarkStrategy<LRRecord> strategy = WatermarkStrategy
                .<LRRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                .withTimestampAssigner((event, timestamp) -> event.creationTS);
        DataStream<LRRecord> dataStream = env
             .fromCollection(records)
             .assignTimestampsAndWatermarks(strategy)
             .name("LR1_Source");

        dataStream.flatMap(new ThroughputLogger<LRRecord>(LRRecord.RECORD_SIZE_IN_BYTE, 1000));

        dataStream
                .map(value -> {
                    value.position = (short) (value.position / 5280);
                    return value;
                })
                .keyBy(new KeySelector<LRRecord, Tuple3<Short, Short,Integer>>() {
                    @Override
                    public Tuple3<Short, Short, Integer> getKey(LRRecord value) throws Exception {
                        return new Tuple3<>(value.highway, value.direction, value.position);
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(300), Duration.ofSeconds(1)))
                .aggregate(new AggregateFunction<LRRecord, Tuple2<Long, Double>, Double>() {
                    @Override
                    public Tuple2<Long, Double> createAccumulator() {
                        return new Tuple2<>(0L, 0.0);
                    }

                    @Override
                    public Tuple2<Long, Double> add(LRRecord value, Tuple2<Long, Double> accumulator) {
                        return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + value.speed);
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
                .filter(avgSpeed -> avgSpeed < 40)
                .name("WindowOperator")
                .sinkTo(new DiscardingSink<Double>() {
                });

        env.execute("LR1");

    }
}
