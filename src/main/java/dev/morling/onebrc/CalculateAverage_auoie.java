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

  public static final class ByteArrayWrapper {
    private final byte[] data;
    private final int hashCode;

    public ByteArrayWrapper(byte[] data) {
      this.data = data;
      this.hashCode = Arrays.hashCode(data);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ByteArrayWrapper that = (ByteArrayWrapper) o;
      return Objects.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
      return hashCode;
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
    HashMap<ByteArrayWrapper, MeasurementAggregator> aggs = new HashMap<>();
    int index = 0;
    while (index < buffer.length) {
      int start = index;
      for (; buffer[index] != ';'; index++) {}
      int end = index;
      var newCityArray = Arrays.copyOfRange(buffer, start, end);
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
      var station = new ByteArrayWrapper(newCityArray);
      var agg = aggs.get(station);
      if (agg == null) {
        agg = new MeasurementAggregator();
        agg.includeValue(intValue);
        aggs.put(station, agg);
      } else {
        agg.includeValue(intValue);
      }
    }
    return aggs;
  }

  private static List<HashMap<ByteArrayWrapper, MeasurementAggregator>> getAllAggregates(
      List<byte[]> buffers) throws ExecutionException, InterruptedException {
    List<HashMap<ByteArrayWrapper, MeasurementAggregator>> allAggs = new ArrayList<>();
    ReentrantLock lock = new ReentrantLock();
    List<Future<?>> tasks = new ArrayList<>();
    try (final var executor =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())) {
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
    List<byte[]> buffers = getChunks(FILE, BATCH_SIZE, INSPECTION_SIZE);
    System.err.println("Getting aggregates for " + buffers.size() + " buffers");
    List<HashMap<ByteArrayWrapper, MeasurementAggregator>> allAggs = getAllAggregates(buffers);
    System.err.println("Finished getting all aggregates");
    var measurements = getResults(allAggs);
    System.out.println(measurements);
  }

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    memoryMappedFile();
  }
}
