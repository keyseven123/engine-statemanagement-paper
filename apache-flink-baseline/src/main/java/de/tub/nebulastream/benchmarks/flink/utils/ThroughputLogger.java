package de.tub.nebulastream.benchmarks.flink.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputLogger<T> extends RichFlatMapFunction<T, T> {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger.class);

    private final long logIntervalMs;

    private long lastLogTime;
    private long recordCountSinceLastLog;
    private long totalRecordCount;

    public ThroughputLogger(long logIntervalMs) {
        this.logIntervalMs = logIntervalMs;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        lastLogTime = System.currentTimeMillis();
        recordCountSinceLastLog = 0;
        totalRecordCount = 0;
    }

    @Override
    public void flatMap(T value, Collector<T> out) throws Exception {
        totalRecordCount++;
        recordCountSinceLastLog++;
        out.collect(value);

        long now = System.currentTimeMillis();
        long timeDiff = now - lastLogTime;
        if (timeDiff >= logIntervalMs) {
            double seconds = timeDiff / 1000.0;
            double tuplesPerSecond = recordCountSinceLastLog / seconds;

            LOG.info("Thread: {} has received {} total tuples and {} tuples in the last {} ms --> {} tuples/sec",
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                    totalRecordCount,
                    recordCountSinceLastLog,
                    timeDiff,
                    tuplesPerSecond);

            lastLogTime = now;
            recordCountSinceLastLog = 0;
        }
    }
}

