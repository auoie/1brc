/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class CalculateAverage_auoie {

  private static final String FILE = "./measurements.txt";

  private record ByteArraySlice(byte[] buffer, int hash, int lo, int hi) {
    @Override
    public boolean equals(Object o) {
      // if (this == o) return true;
      // if (o == null || getClass() != o.getClass()) return false;
      ByteArraySlice that = (ByteArraySlice) o;
      int dif = hi - lo;
      if (that.hi - that.lo != dif) {
        return false;
      }
      for (int i = 0; i <= dif; i++) {
        if (buffer[lo + i] != that.buffer[that.lo + i]) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      return new String(Arrays.copyOfRange(buffer, lo, hi), StandardCharsets.UTF_8);
    }

    public ByteArrayWrapper getByteArrayWrapper() {
      return new ByteArrayWrapper(Arrays.copyOfRange(buffer, lo, hi), hash);
    }
  }

  private record ByteArrayWrapper(byte[] data, int hash) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ByteArrayWrapper that = (ByteArrayWrapper) o;
      return Objects.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      return new String(data, StandardCharsets.UTF_8);
    }
  }

  private record ResultRow(double min, double mean, double max) {

    @Override
    public String toString() {
      return round(min) + "/" + round(mean) + "/" + round(max);
    }

    private double round(double value) {
      return Math.round(value * 10.0) / 10.0;
    }
  }

  private static class MeasurementAggregator {
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long sum;
    private long count;

    private void includeValue(long value) {
      min = Math.min(min, value);
      max = Math.max(max, value);
      sum += value;
      count += 1;
    }

    private void includeAggregate(MeasurementAggregator agg) {
      min = Math.min(agg.min, min);
      max = Math.max(agg.max, max);
      sum += agg.sum;
      count += agg.count;
    }
  }

  private static List<byte[]> getChunks(String fileName, int batch_size, int inspection_size)
      throws IOException {
    List<byte[]> buffers = new ArrayList<>();
    try (FileChannel channel = new FileInputStream(fileName).getChannel()) {
      for (long i = 0; i < channel.size(); ) {
        final long bufSize = Math.min(channel.size() - i, batch_size + inspection_size);
        MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, i, bufSize);
        final int start_inspect = (int) Math.min(batch_size, bufSize - 1);
        int dif = 0;
        while (buffer.get(start_inspect + dif) != '\n') {
          dif++;
        }
        int bufLength = start_inspect + dif + 1;
        byte[] byteBuf = new byte[bufLength];
        buffer.get(byteBuf, 0, bufLength);
        buffers.add(byteBuf);
        i += bufLength;
      }
    }
    return buffers;
  }

  private static HashMap<ByteArrayWrapper, MeasurementAggregator> getAggregateForBuffer(
      byte[] buffer) {
    HashMap<ByteArraySlice, MeasurementAggregator> aggs = new HashMap<>();
    int index = 0;
    while (index < buffer.length) {
      int start = index;
      int hash = 0;
      for (; buffer[index] != ';'; index++) {
        hash = 31 * hash + buffer[index];
      }
      int end = index;
      index++;
      boolean sign = true;
      if (buffer[index] == '-') {
        sign = false;
        index++;
      }
      int intValue = 0;
      for (; buffer[index] != '\n'; index++) {
        if (buffer[index] != '.') {
          intValue = 10 * intValue + (buffer[index] - '0');
        }
      }
      if (!sign) {
        intValue = -intValue;
      }
      index++;
      var station = new ByteArraySlice(buffer, hash, start, end);
      var agg = aggs.get(station);
      if (agg == null) {
        agg = new MeasurementAggregator();
        agg.includeValue(intValue);
        aggs.put(station, agg);
      } else {
        agg.includeValue(intValue);
      }
    }
    HashMap<ByteArrayWrapper, MeasurementAggregator> result = new HashMap<>();
    for (var pair : aggs.entrySet()) {
      result.put(pair.getKey().getByteArrayWrapper(), pair.getValue());
    }
    return result;
  }

  private static List<HashMap<ByteArrayWrapper, MeasurementAggregator>> getAllAggregates(
      List<byte[]> buffers) throws ExecutionException, InterruptedException {
    List<HashMap<ByteArrayWrapper, MeasurementAggregator>> allAggs = new ArrayList<>();
    ReentrantLock lock = new ReentrantLock();
    List<Future<?>> tasks = new ArrayList<>();
    int numWorkers = Runtime.getRuntime().availableProcessors();
    try (final var executor = Executors.newFixedThreadPool(numWorkers)) {
      for (var buffer : buffers) {
        var task =
            executor.submit(
                () -> {
                  var aggs = getAggregateForBuffer(buffer);
                  try {
                    lock.lock();
                    allAggs.add(aggs);
                  } finally {
                    lock.unlock();
                  }
                });
        tasks.add(task);
      }
      for (var task : tasks) {
        task.get();
      }
    }
    return allAggs;
  }

  private static TreeMap<String, ResultRow> getResults(
      List<HashMap<ByteArrayWrapper, MeasurementAggregator>> allAggs) {
    HashMap<ByteArrayWrapper, MeasurementAggregator> results = new HashMap<>();
    for (var map : allAggs) {
      map.forEach(
          (station, agg) -> {
            var curAgg = results.get(station);
            if (curAgg == null) {
              results.put(station, agg);
            } else {
              curAgg.includeAggregate(agg);
            }
          });
    }
    TreeMap<String, ResultRow> measurements = new TreeMap<>();
    results.forEach(
        (station, agg) -> {
          var resultRow =
              new ResultRow(agg.min / 10.0, (agg.sum / 10.0) / agg.count, agg.max / 10.0);
          measurements.put(station.toString(), resultRow);
        });
    return measurements;
  }

  private static void memoryMappedFile()
      throws IOException, ExecutionException, InterruptedException {
    final int BATCH_SIZE = 128 * 1024 * 1024;
    final int INSPECTION_SIZE = 128 * 1024;
    System.err.println("Getting buffers");
    long t1 = System.currentTimeMillis();
    List<byte[]> buffers = getChunks(FILE, BATCH_SIZE, INSPECTION_SIZE);
    long t2 = System.currentTimeMillis();
    System.err.println("Got buffers in ms: " + (t2 - t1));
    System.err.println("Getting aggregates for " + buffers.size() + " buffers");
    List<HashMap<ByteArrayWrapper, MeasurementAggregator>> allAggs = getAllAggregates(buffers);
    long t3 = System.currentTimeMillis();
    System.err.println("Finished getting all aggregates in ms: " + (t3 - t2));
    var measurements = getResults(allAggs);
    System.out.println(measurements);
  }

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    memoryMappedFile();
  }
}
