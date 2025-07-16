package de.tub.nebulastream.benchmarks.flink.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {

    public static class Result {

        SummaryStatistics throughputs;
        Map<String, SummaryStatistics> perHostThr;

        public Result(SummaryStatistics throughputs, Map<String, SummaryStatistics> perHostThr) {
            this.throughputs = throughputs;
            this.perHostThr = perHostThr;
        }
    }

    public static Result analyze(String file) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(file));

        Pattern throughputLinePattern = Pattern.compile(
            ".*Thread: (\\d+) has received .*? (\\d+) tuples in the last (\\d+) ms.*"
        );

        SummaryStatistics globalStats = new SummaryStatistics();
        Map<String, SummaryStatistics> perThreadStats = new HashMap<>();

        while (sc.hasNextLine()) {
            String line = sc.nextLine();

            Matcher matcher = throughputLinePattern.matcher(line);
            if (matcher.matches()) {
                String threadId = matcher.group(1);
                long tuplesInWindow = Long.parseLong(matcher.group(2));
                long timeMs = Long.parseLong(matcher.group(3));

                if (timeMs > 0) {
                    double throughput = (tuplesInWindow / (double) timeMs) * 1000.0;
                    globalStats.addValue(throughput);

                    perThreadStats
                        .computeIfAbsent(threadId, k -> new SummaryStatistics())
                        .addValue(throughput);
                }
            }
        }

        return new Result(globalStats, perThreadStats);
    }

    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        String benchmarkName = args[1];
        String workerThreads = args[2];
        Result r1 = analyze(inputFile);
        SummaryStatistics throughputs = r1.throughputs;

        System.err.println("================= Throughput ("+r1.perHostThr.size()+" reports ) =====================");
        double sumThroughput = 0;
        for(Map.Entry<String, SummaryStatistics> entry : r1.perHostThr.entrySet()) {
            System.err.println("====== "+entry.getKey()+" (entries: "+entry.getValue().getN()+")=======");
            System.err.println("Mean throughput " + entry.getValue().getMean());
            sumThroughput = sumThroughput + entry.getValue().getMean();
        }
        System.err.println("Sum throughput " + sumThroughput);

        FileOutputStream fos = new FileOutputStream("./"+benchmarkName + ".csv", true);
        fos.write(benchmarkName.getBytes());
        fos.write(",".getBytes());
        fos.write(workerThreads.getBytes());
        fos.write(",".getBytes());
        fos.write(Long.toString(Math.round(sumThroughput)).getBytes());
        fos.write("\n".getBytes());
        fos.close();


    }
}