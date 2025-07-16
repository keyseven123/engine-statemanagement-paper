package de.tub.nebulastream.benchmarks.flink.util;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class MemorySource<T extends Serializable> implements Source<T, MemorySource.GenericCsvSplit, Void> {

    private final List<T> parsedRecords = new ArrayList<>();
    private final long maxRecords;


    public MemorySource(String filePath, long maxRecords, Class<T> clazz, CsvSchema schema) {
        this(filePath, maxRecords, clazz, schema, false);
    }

    public MemorySource(String filePath, long maxRecords, Class<T> clazz, CsvSchema schema, boolean skipFirstLine) {
        this.maxRecords = maxRecords;
        CsvMapper mapper = new CsvMapper();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            long count = 0;
            if (skipFirstLine) {
                line = br.readLine();
            }

            while ((line = br.readLine()) != null && count < maxRecords) {
                T record = mapper.readerFor(clazz).with(schema).readValue(line);
                parsedRecords.add(record);
                count++;
            }
            System.out.println("MemorySource: Loaded " + parsedRecords.size() + " records into memory of " +  maxRecords + " maxRecords");
        } catch (IOException e) {
            throw new RuntimeException("Failed to read or parse CSV", e);
        }
    }

    // ---- Source Implementation ----

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, GenericCsvSplit> createReader(SourceReaderContext readerContext) {
        return new InMemoryReader();
    }

    @Override
    public SplitEnumerator<GenericCsvSplit, Void> createEnumerator(SplitEnumeratorContext<GenericCsvSplit> ctx) {
        return new SingleSplitEnumerator(ctx);
    }

    @Override
    public SplitEnumerator<GenericCsvSplit, Void> restoreEnumerator(SplitEnumeratorContext<GenericCsvSplit> ctx, Void checkpoint) {
        return createEnumerator(ctx);
    }

    @Override
    public SimpleVersionedSerializer<GenericCsvSplit> getSplitSerializer() {
        return new GenericCsvSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<Void>() {
            public int getVersion() { return 1; }
            public byte[] serialize(Void obj) { return new byte[0]; }
            public Void deserialize(int version, byte[] serialized) { return null; }
        };
    }

    // ---- SourceReader ----
    private class InMemoryReader implements SourceReader<T, GenericCsvSplit> {
        private int index = 0;
        private static final int BATCH_SIZE = 1000;

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<T> output) {
            if (index >= parsedRecords.size()) {
                System.out.println("END_OF_INPUT at " + System.currentTimeMillis());
                return InputStatus.END_OF_INPUT;
            }

            int emitted = 0;
            while (index < parsedRecords.size() && emitted < BATCH_SIZE) {
                output.collect(parsedRecords.get(index++));
                emitted++;
            }

            return (index >= parsedRecords.size()) ? InputStatus.END_OF_INPUT : InputStatus.MORE_AVAILABLE;
        }
        @Override
        public List<GenericCsvSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void addSplits(List<GenericCsvSplit> splits) {
            // No-op: all data is already in memory
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {}

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }
    }

    // ---- SplitEnumerator ----
    private class SingleSplitEnumerator implements SplitEnumerator<GenericCsvSplit, Void> {
        private final SplitEnumeratorContext<GenericCsvSplit> ctx;
        private boolean assigned = false;

        public SingleSplitEnumerator(SplitEnumeratorContext<GenericCsvSplit> ctx) {
            this.ctx = ctx;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String host) {
            if (!assigned) {
                ctx.assignSplit(new GenericCsvSplit("split-0"), subtaskId);
                ctx.signalNoMoreSplits(subtaskId);
                assigned = true;
            }
        }

        @Override public void addReader(int subtaskId) {}
        @Override public void addSplitsBack(List<GenericCsvSplit> splits, int subtaskId) {}
        @Override public Void snapshotState(long checkpointId) { return null; }
        @Override public void close() {}
    }

    // ---- Split Types ----
    public static class GenericCsvSplit implements SourceSplit {
        private final String id;

        public GenericCsvSplit(String id) {
            this.id = id;
        }

        @Override
        public String splitId() {
            return id;
        }
    }

    public static class GenericCsvSplitSerializer implements SimpleVersionedSerializer<GenericCsvSplit> {
        @Override public int getVersion() { return 1; }

        @Override
        public byte[] serialize(GenericCsvSplit split) {
            return split.splitId().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public GenericCsvSplit deserialize(int version, byte[] serialized) {
            return new GenericCsvSplit(new String(serialized, StandardCharsets.UTF_8));
        }
    }
}
