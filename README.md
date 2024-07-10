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
# openjdk 21.0.3 2024-04-16
# OpenJDK Runtime Environment (build 21.0.3+9)
# OpenJDK 64-Bit Server VM (build 21.0.3+9, mixed mode, sharing)
javac src/main/java/dev/morling/onebrc/CalculateAverage_auoie.java -d target/classes
time java -cp target/classes dev.morling.onebrc.CalculateAverage_auoie > mysolution.txt
```

```text
Getting tasks
Got 823 tasks in ms: 32
Finished getting all aggregates in ms: 6589
java -cp target/classes dev.morling.onebrc.CalculateAverage_auoie >   96.45s user 6.46s system 1430% cpu 7.194 total
```

## GraalVM

### Getting

```bash
curl -OL https://download.oracle.com/graalvm/21/latest/graalvm-jdk-21_linux-x64_bin.tar.gz
tar xvf graalvm-jdk-21_linux-x64_bin.tar.gz
export PATH=$(pwd)/graalvm-jdk-21.0.3+7.1/bin:$PATH
export JAVA_HOME=$(pwd)/graalvm-jdk-21.0.3+7.1
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
Got 823 tasks in ms: 72
Finished getting all aggregates in ms: 3863
java -cp target/classes dev.morling.onebrc.CalculateAverage_auoie >   59.19s user 1.81s system 1162% cpu 5.249 total
```

### GraalVM Native Binary

```bash
native-image -cp ./target/classes dev.morling.onebrc.CalculateAverage_auoie
time ./dev.morling.onebrc.calculateaverage_auoie > mysolution.txt
```

```text
Getting tasks
Got 823 tasks in ms: 14
Finished getting all aggregates in ms: 3670
./dev.morling.onebrc.calculateaverage_auoie > mysolution.txt  48.89s user 2.55s system 1316% cpu 3.909 total
```
