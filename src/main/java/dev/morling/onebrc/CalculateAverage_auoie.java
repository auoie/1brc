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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
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
      for (int i = 0; i < dif; i++) {
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
    private long min;
    private long max;
    private long sum;
    private long count;

    public MeasurementAggregator(int value) {
      sum = value;
      min = value;
      max = value;
      count = 1;
    }

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

  private record Task(MappedByteBuffer buffer, int length) {}

  private static List<Task> getTasks(String fileName, int batch_size, int inspection_size)
      throws IOException {
    List<Task> tasks = new ArrayList<>();
    {
      try (FileInputStream fileStream = new FileInputStream(fileName);
          FileChannel channel = fileStream.getChannel()) {
        for (long i = 0; i < channel.size(); ) {
          final long bufSize = Math.min(channel.size() - i, batch_size + inspection_size);
          MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, i, bufSize);
          final int start_inspect = (int) Math.min(batch_size, bufSize - 1);
          int dif = 0;
          while (buffer.get(start_inspect + dif) != '\n') {
            dif++;
          }
          int bufLength = start_inspect + dif + 1;
          Task task = new Task(buffer, bufLength);
          tasks.add(task);
          i += bufLength;
        }
      }
    }
    return tasks;
  }

  private static List<List<Entry<ByteArrayWrapper, MeasurementAggregator>>> getAllAggregates(
      List<Task> tasks, int maxBufferSize, int numWorkers) throws InterruptedException {
    ConcurrentLinkedQueue<Task> taskQueue = new ConcurrentLinkedQueue<>(tasks);
    List<List<Entry<ByteArrayWrapper, MeasurementAggregator>>> allAggs = new ArrayList<>();
    ReentrantLock lock = new ReentrantLock();
    List<Thread> threadPool = new ArrayList<>();
    Runnable runnable =
        () -> {
          byte[] buffer = new byte[maxBufferSize];
          while (true) {
            var task = taskQueue.poll();
            if (task == null) {
              break;
            }
            task.buffer.get(buffer, 0, task.length);
            var aggs = getAggregateForBuffer(buffer, task.length);
            try {
              lock.lock();
              allAggs.add(aggs);
            } finally {
              lock.unlock();
            }
          }
        };
    for (int i = 0; i < numWorkers - 1; i++) {
      threadPool.add(new Thread(runnable));
    }
    for (var thread : threadPool) {
      thread.start();
    }
    runnable.run();
    for (var thread : threadPool) {
      thread.join();
    }
    return allAggs;
  }

  private record Entry<K, V>(K key, V value) {}

  private static List<Entry<ByteArrayWrapper, MeasurementAggregator>> getAggregateForBuffer(
      byte[] buffer, int length) {
    HashMap<ByteArraySlice, MeasurementAggregator> aggs = new HashMap<>();
    int index = 0;
    while (index < length) {
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
      int intValue = buffer[index] - '0';
      index++;
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
        aggs.put(station, new MeasurementAggregator(intValue));
      } else {
        agg.includeValue(intValue);
      }
    }
    List<Entry<ByteArrayWrapper, MeasurementAggregator>> entries = new ArrayList<>();
    aggs.forEach(
        (key, value) -> {
          entries.add(new Entry<>(key.getByteArrayWrapper(), value));
        });
    return entries;
  }

  private static TreeMap<String, ResultRow> getResults(
      List<List<Entry<ByteArrayWrapper, MeasurementAggregator>>> allAggs) {
    HashMap<ByteArrayWrapper, MeasurementAggregator> results = new HashMap<>();
    for (var map : allAggs) {
      for (var entry : map) {
        var station = entry.key;
        var value = entry.value;
        var curAgg = results.get(station);
        if (curAgg == null) {
          results.put(station, value);
        } else {
          curAgg.includeAggregate(value);
        }
      }
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

  private static void memoryMappedFile() throws IOException, InterruptedException {
    final int BATCH_SIZE = 16 * 1024 * 1024;
    final int INSPECTION_SIZE = 128 * 1024;
    System.err.println("Getting tasks");
    long t0 = System.currentTimeMillis();
    var tasks = getTasks(FILE, BATCH_SIZE, INSPECTION_SIZE);
    long t1 = System.currentTimeMillis();
    System.err.println("Got " + tasks.size() + " tasks in ms: " + (t1 - t0));
    int numWorkers = Runtime.getRuntime().availableProcessors();
    var allAggs = getAllAggregates(tasks, BATCH_SIZE + INSPECTION_SIZE, numWorkers);
    long t2 = System.currentTimeMillis();
    System.err.println("Finished getting all aggregates in ms: " + (t2 - t1));
    var measurements = getResults(allAggs);
    System.out.println(measurements);
  }

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    memoryMappedFile();
  }
}
