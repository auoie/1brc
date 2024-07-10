# One Billion Row Challenge

Techniques used:

- Map file to memory. Splits the file into approximately 64MB tasks, where each task ends in a new
  line.
- Send tasks to a queue being read a thread pool. The threads exit when the queue is empty. These
  threads saturate the CPU threads.
- Each thread reuses a buffer to read from the file. This saves memory.
- Uses custom parser to read in byte arrays for the city names and calculate the double values
- Uses custom byte array class instead of string for hashing keys. Additionally, uses a custom open
  hashset with a predefined size to make inserting and retrieval operations faster. This is stolen
  from [a QuestDB blog](https://questdb.io/blog/billion-row-challenge-step-by-step/).

## Setup

```bash
cp measurements.txt /tmp
mv measurements.txt measurements.txt.back
ln -s /tmp/measurements.txt measurements.txt
```

## OpenJDK

```bash
java --version
# openjdk 21.0.3 2024-04-16 LTS
# OpenJDK Runtime Environment Corretto-21.0.3.9.1 (build 21.0.3+9-LTS)
# OpenJDK 64-Bit Server VM Corretto-21.0.3.9.1 (build 21.0.3+9-LTS, mixed mode, sharing)
javac src/main/java/dev/morling/onebrc/CalculateAverage_auoie.java -d target/classes
time java -cp target/classes dev.morling.onebrc.CalculateAverage_auoie > mysolution.txt
```

```text
Getting tasks
Got 823 tasks in ms: 33
Finished getting all aggregates in ms: 6808
java -cp target/classes dev.morling.onebrc.CalculateAverage_auoie >   95.09s user 10.61s system 1420% cpu 7.444 total
```

## GraalVM

### Getting

```bash
sdk install java 21.0.3-graal
java --version
# java 21.0.3 2024-04-16 LTS
# Java(TM) SE Runtime Environment Oracle GraalVM 21.0.3+7.1 (build 21.0.3+7-LTS-jvmci-23.1-b37)
# Java HotSpot(TM) 64-Bit Server VM Oracle GraalVM 21.0.3+7.1 (build 21.0.3+7-LTS-jvmci-23.1-b37, mixed mode, sharing)
```

### GraalVM

```bash
javac src/main/java/dev/morling/onebrc/CalculateAverage_auoie.java -d target/classes
time java -cp target/classes dev.morling.onebrc.CalculateAverage_auoie > mysolution.txt
```

```text
Getting tasks
Got 823 tasks in ms: 29
Finished getting all aggregates in ms: 2940
java -cp target/classes dev.morling.onebrc.CalculateAverage_auoie >   44.30s user 1.83s system 1302% cpu 3.543 total
```

### GraalVM Native Binary

```bash
native-image -cp ./target/classes dev.morling.onebrc.CalculateAverage_auoie
time ./dev.morling.onebrc.calculateaverage_auoie > mysolution.txt
```

```text
Getting tasks
Got 823 tasks in ms: 11
Finished getting all aggregates in ms: 3533
./dev.morling.onebrc.calculateaverage_auoie > mysolution.txt  46.42s user 2.97s system 1299% cpu 3.800 total
```
