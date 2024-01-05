package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class CalculateAverage_flamparski {
    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private static class Chunk {
        private int index;
        private byte[] bytes;
        private int length;

        private Chunk(int index, int size) {
            this.index = index;
            this.bytes = new byte[size];
        }
    }

    private static record MeasurementChunk(
                                           int index,
                                           byte[] prefix,
                                           byte[] suffix) {
    }

    private static Map<String, MeasurementAggregator> measurements = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        var numChunks = 8;
        var numCores = Runtime.getRuntime().availableProcessors();
        final var raf = new RandomAccessFile(FILE, "r");
        var fileSize = raf.length();
        var fileChunkSize = (int) (fileSize / numChunks);

        var prefixes = new ArrayList<byte[]>();
        var suffixes = new ArrayList<byte[]>();

        var chunks = IntStream.range(0, numChunks + 1)
                .mapToObj(index -> {
                    var chunk = new Chunk(index, fileChunkSize);
                    try {
                        var read = 0;
                        var last = 0;
                        while (last > -1 && read < fileChunkSize) {
                            read += last;
                            last = raf.read(chunk.bytes, 0, fileChunkSize - read);
                        }
                        chunk.length = read;
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return chunk;
                })
                .map(fileChunk -> {
                    var threadChunkSize = fileChunk.bytes.length / numCores;
                    var myPrefixes = new byte[numCores + 1][];
                    var mySuffixes = new byte[numCores + 1][];
                    IntStream.range(0, numCores + 1).parallel().forEach(i -> {
                        var start = i * threadChunkSize;
                        var end = (i + 1) * threadChunkSize;
                        byte[] prefix = null;
                        var j = start;
                        var lastSemi = -1;
                        for (; j < end && j < fileChunk.length; j++) {
                            var chr = fileChunk.bytes[j];
                            if (chr == '\n') {
                                if (prefix == null) {
                                    prefix = Arrays.copyOfRange(fileChunk.bytes, start, j);
                                }
                                else if (lastSemi != -1) {
                                    var name = new String(fileChunk.bytes, start, lastSemi - start, StandardCharsets.UTF_8);
                                    var value = new String(fileChunk.bytes, lastSemi + 1, j - lastSemi - 1, StandardCharsets.UTF_8);
                                    addMeasurement(name, value);
                                    lastSemi = -1;
                                }
                                else {
                                    System.err.printf("chunk %d ERR: newline detected but there is no last semi and prefix already found: %s\n", fileChunk.index,
                                            new String(Arrays.copyOfRange(fileChunk.bytes, start, j)));
                                }
                                start = j + 1;
                            }
                            else if (chr == ';') {
                                lastSemi = j;
                            }
                        }
                        byte[] suffix = Arrays.copyOfRange(fileChunk.bytes, start, j);
                        myPrefixes[i] = prefix;
                        mySuffixes[i] = suffix;
                    });
                    var prefix = myPrefixes[0];
                    var suffix = mySuffixes[mySuffixes.length - 1];
                    for (var j = 0; j < myPrefixes.length - 1; j++) {
                        var p = myPrefixes[j + 1];
                        var s = mySuffixes[j];
                        if (p != null && s != null) {
                            var buf = new byte[p.length + s.length];
                            System.arraycopy(s, 0, buf, 0, s.length);
                            System.arraycopy(p, 0, buf, s.length, p.length);
                            var str = new String(buf, StandardCharsets.UTF_8);
                            var parts = str.split(";");
                            addMeasurement(parts[0], parts[1]);
                        }
                    }

                    return new MeasurementChunk(fileChunk.index, prefix, suffix);
                })
                .toList();

        raf.close();

        chunks.forEach(chunk -> {
            prefixes.add(chunk.prefix);
            suffixes.add(chunk.suffix);
        });

        var prefix = prefixes.getFirst();
        for (var i = 0; i < prefixes.size() - 1; i++) {
            var p = prefixes.get(i + 1);
            var s = suffixes.get(i);
            if (p != null && s != null) {
                var buf = new byte[p.length + s.length];
                System.arraycopy(s, 0, buf, 0, s.length);
                System.arraycopy(p, 0, buf, s.length, p.length);
                var str = new String(buf, StandardCharsets.UTF_8);
                var parts = str.split(";");
                addMeasurement(parts[0], parts[1]);
            }
        }

        var parts = new String(prefix, StandardCharsets.UTF_8).split(";");
        addMeasurement(parts[0], parts[1]);

        var results = measurements.entrySet().stream().sorted(Comparator.comparing(e -> e.getKey())).map(entry -> {
            var name = entry.getKey();
            var min = entry.getValue().min;
            var max = entry.getValue().max;
            var mean = entry.getValue().sum / entry.getValue().count;
            return String.format("%s=%.1f/%.1f/%.1f", name, min, mean, max);
        }).toList();
        var resultSb = new StringBuffer(results.toString());
        resultSb.setCharAt(0, '{');
        resultSb.setCharAt(resultSb.length() - 1, '}');
        System.out.println(resultSb.toString());
    }

    private static void addMeasurement(String name, String value) {
        var doubleValue = Double.valueOf(value);
        if (!measurements.containsKey(name)) {
            measurements.put(name, new MeasurementAggregator());
        }
        var agg = measurements.get(name);
        agg.count++;
        agg.sum += doubleValue;
        agg.max = Math.max(agg.max, doubleValue);
        agg.min = Math.min(agg.min, doubleValue);
    }
}
