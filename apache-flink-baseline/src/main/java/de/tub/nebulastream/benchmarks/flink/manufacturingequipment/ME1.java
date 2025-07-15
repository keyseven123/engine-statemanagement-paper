package de.tub.nebulastream.benchmarks.flink.manufacturingequipment;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.manufacturingequipment.MERecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import java.time.Duration;
import org.slf4j.Logger;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import java.io.File;
import java.util.List;


public class ME1 {

    private static final Logger LOG = LoggerFactory.getLogger(ME1.class);

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

        List<MERecord> records = MERecord.loadFromCsv("/tmp/data/manufacturing_1G.csv", numOfRecords);
        WatermarkStrategy<MERecord> strategy = WatermarkStrategy
                .<MERecord>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // We have no out-of-orderness in the dataset
                .withTimestampAssigner((event, timestamp) -> event.creationTS);
        DataStream<MERecord> dataStream = env
             .fromCollection(records)
                .assignTimestampsAndWatermarks(strategy)
             .name("ME1_Source");

        dataStream.flatMap(new ThroughputLogger<MERecord>(MERecord.RECORD_SIZE_IN_BYTE, 100_000));
        AllWindowedStream<MERecord, TimeWindow> windowedStream = dataStream
                        .windowAll(SlidingProcessingTimeWindows.of(Duration.ofSeconds(60), Duration.ofSeconds(1)));

        SingleOutputStreamOperator<Double> avg_mf01 = windowedStream.aggregate(new AggregateFunction<MERecord, Tuple2<Long, Double>, Double>() {
            @Override
            public Tuple2<Long, Double> createAccumulator() {
                return new Tuple2<>(0L, 0.0);
            }

            @Override
            public Tuple2<Long, Double> add(MERecord value, Tuple2<Long, Double> accumulator) {
                return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + value.mf01);
            }

            @Override
            public Double getResult(Tuple2<Long, Double> accumulator) {
                return accumulator.f1 / accumulator.f0;
            }

            @Override
            public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
            }
        });

        SingleOutputStreamOperator<Double> avg_mf02 = windowedStream.aggregate(new AggregateFunction<MERecord, Tuple2<Long, Double>, Double>() {
            @Override
            public Tuple2<Long, Double> createAccumulator() {
                return new Tuple2<>(0L, 0.0);
            }

            @Override
            public Tuple2<Long, Double> add(MERecord value, Tuple2<Long, Double> accumulator) {
                return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + value.mf02);
            }

            @Override
            public Double getResult(Tuple2<Long, Double> accumulator) {
                return accumulator.f1 / accumulator.f0;
            }

            @Override
            public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
            }
        });

        SingleOutputStreamOperator<Double> avg_mf03 = windowedStream.aggregate(new AggregateFunction<MERecord, Tuple2<Long, Double>, Double>() {
            @Override
            public Tuple2<Long, Double> createAccumulator() {
                return new Tuple2<>(0L, 0.0);
            }

            @Override
            public Tuple2<Long, Double> add(MERecord value, Tuple2<Long, Double> accumulator) {
                return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + value.mf03);
            }

            @Override
            public Double getResult(Tuple2<Long, Double> accumulator) {
                return accumulator.f1 / accumulator.f0;
            }

            @Override
            public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
            }
        });

        avg_mf01.union(avg_mf02, avg_mf03)
                .sinkTo(new DiscardingSink<Double>() {
                });

        env.execute("ME1");

    }
}
