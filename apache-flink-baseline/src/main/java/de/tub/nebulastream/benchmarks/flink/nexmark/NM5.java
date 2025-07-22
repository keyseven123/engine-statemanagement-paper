package de.tub.nebulastream.benchmarks.flink.nexmark;

import de.tub.nebulastream.benchmarks.flink.utils.ThroughputLogger;
import de.tub.nebulastream.benchmarks.flink.nexmark.NMBidRecord;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.ParameterTool;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import de.tub.nebulastream.benchmarks.flink.util.MemorySource;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.io.File;
import java.time.Duration;
import java.util.List;

public class NM5 {

    private static final Logger LOG = LoggerFactory.getLogger(NM5.class);

    /**
     * SELECT start, end, start, end, auctionId, num, start, end, max_tmp
     * FROM (SELECT auctionId, COUNT(auctionId) AS num, start, end
     * FROM bid
     * GROUP BY auctionId
     * WINDOW SLIDING(timestamp, SIZE 10 SEC, ADVANCE BY 2 SEC))
     * INNMR JOIN (SELECT auctionId, MAX(num_ids) AS max_tmp, start, end
     * FROM
     * (SELECT auctionId, COUNT(auctionId) AS num_ids, start
     * FROM bid
     * GROUP BY auctionId
     * WINDOW SLIDING(timestamp, SIZE 10 SEC, ADVANCE BY 2 SEC))
     * WINDOW TUMBLING(start, SIZE 2 SEC))
     * ON num >= max_tmp
     * WINDOW TUMBLING(start, SIZE 2 SEC)
     * INTO CHECKSUM;
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

        WatermarkStrategy<NMBidRecord> strategy = WatermarkStrategy
                .<NMBidRecord>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> event.timestamp / 1000);
        MemorySource<NMBidRecord> source = new MemorySource<NMBidRecord>(basePathForDataFiles + "/bid_more_data_6GB.csv", numOfRecords, NMBidRecord.class, NMBidRecord.schema);
        DataStream<NMBidRecord> sourceStream = env
            .fromSource(source, strategy, "Bid_Source")
            .returns(TypeExtractor.getForClass(NMBidRecord.class))
            .setParallelism(1);


        DataStream<NMBidRecord> dataStream = sourceStream.flatMap(new ThroughputLogger<NMBidRecord>(500));

        // auctionId, num, start, end
        DataStream<Tuple4<Integer, Integer, Long, Long>> countStream =
                dataStream.keyBy(new KeySelector<NMBidRecord, Integer>() {
                            @Override
                            public Integer getKey(NMBidRecord rec) throws Exception {
                                return rec.auctionId;
                            }
                        })
                        .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(2))) // WINDOW SLIDING(timestamp, SIZE 10 SEC, ADVANCE BY 2 SEC)
                        .process(new ProcessWindowFunction<NMBidRecord, Tuple4<Integer, Integer, Long, Long>, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer key, Context context, Iterable<NMBidRecord> recs, Collector<Tuple4<Integer, Integer, Long, Long>> out) {
                                int count = 0;
                                for (NMBidRecord bid : recs) {
                                    count++;
                                }
                                out.collect(new Tuple4<>(key, count, context.window().getStart(), context.window().getEnd()));
                            }
                        });


        // auctionId, max_tmp, start, end
        DataStream<Tuple4<Integer, Integer, Long, Long>> maxStream =
                countStream.keyBy(new KeySelector<Tuple4<Integer, Integer, Long, Long>, Integer>() {
                            @Override
                            public Integer getKey(Tuple4<Integer, Integer, Long, Long> countRec) throws Exception {
                                return countRec.f0; // auctionId
                            }
                        })
                        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(2))) // WINDOW TUMBLING(start, SIZE 2 SEC)
                        .process(new ProcessWindowFunction<Tuple4<Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer key, Context context, Iterable<Tuple4<Integer, Integer, Long, Long>> recs, Collector<Tuple4<Integer, Integer, Long, Long>> out) {
                                // Find max count
                                int maxCount = 0;
                                for (Tuple4<Integer, Integer, Long, Long> countRec : recs) {
                                    if (countRec.f2 == context.window().getStart() && countRec.f1 >= maxCount) {
                                        maxCount = countRec.f1; // num
                                    }
                                }
                                out.collect(new Tuple4<>(key, maxCount, context.window().getStart(), context.window().getEnd()));
                            }
                        });

        countStream.join(maxStream)
                .where(new KeySelector<Tuple4<Integer, Integer, Long, Long>, Integer>() {
                    @Override
                    public Integer getKey(Tuple4<Integer, Integer, Long, Long> countRec) throws Exception {
                        return countRec.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple4<Integer, Integer, Long, Long>, Integer>() {
                    @Override
                    public Integer getKey(Tuple4<Integer, Integer, Long, Long> maxRec) throws Exception {
                        return maxRec.f0;
                    }
                }) // join on auctionId
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(2)))
                .apply(new FlatJoinFunction<Tuple4<Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple5<Integer, Integer, Long, Long, Integer>>() {
                    @Override
                    public void join(Tuple4<Integer, Integer, Long, Long> countRec, Tuple4<Integer, Integer, Long, Long> maxRec, Collector<Tuple5<Integer, Integer, Long, Long, Integer>> out) throws Exception {
                        if (countRec.f2 == maxRec.f2 && countRec.f1 >= maxRec.f1) { // num >= max_tmp
                            out.collect(new Tuple5<>(countRec.f0, countRec.f1, countRec.f2, countRec.f3, maxRec.f1)); // auctionId, num, start, end, max_tmp
                        }
                    }
                })
                .sinkTo(new DiscardingSink<Tuple5<Integer, Integer, Long, Long, Integer>>() {
                });

        // Sleep maxRuntimeInSeconds seconds and then cancel
        JobClient jobClient = env.executeAsync("NM5");
        LOG.info("Started flink job");
        Thread.sleep(maxRuntimeInSeconds * 1000);
        jobClient.cancel().thenRun(() ->
            LOG.info("Job cancelled after {} seconds.", maxRuntimeInSeconds)
        );

    }
}
