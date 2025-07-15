package de.tub.nebulastream.benchmarks.flink.utils;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputLogger<T> extends RichFlatMapFunction<T, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger.class);

    private long totalReceived = 0;
    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private int elementSize;
    private long logfreq;
    private long query = 0;

    public ThroughputLogger(int elementSize, long logfreq) {
        this.elementSize = elementSize;
        this.logfreq = logfreq;
    }

    public ThroughputLogger(int elementSize, long logfreq, long query) {
        this.elementSize = elementSize;
        this.logfreq = logfreq;
        this.query = query;
    }

    @Override
    public void flatMap(T element, Collector<Integer> collector) throws Exception {
        totalReceived++;
        if (totalReceived % logfreq == 0) {
            // throughput over entire time
            long now = System.currentTimeMillis();
            // throughput for the last "logfreq" elements
            if (lastLogTimeMs == -1) {
                // init (the first)
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            } else {
                long timeDiff = now - lastLogTimeMs;
                long elementDiff = totalReceived - lastTotalReceived;
                double ex = (1000 / (double) timeDiff);

                LOG.info("Worker: {} During the last {} ms, we received {} elements. That's {} elements/second/core. {} MB/sec/core. GB received {}. Query {}",  getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                        timeDiff, elementDiff, elementDiff * ex, elementDiff * ex * elementSize / 1024 / 1024, (totalReceived * elementSize) / 1024 / 1024 / 1024, query);
                // reinit
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            }
        }
    }
}
