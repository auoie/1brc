# One Billion Row Challenge

Techniques used:

- Map file to memory. Splits the file into approximately 64MB tasks, where each task ends in a new line.
- Send tasks to a queue being read a thread pool. The threads exit when the queue is empty. These threads saturate the CPU threads.
- Each thread reuses a buffer to read from the file. This saves memory.
- Uses custom parser to read in byte arrays for the city names and calculate the double values
- Uses custom byte array class instead of string for hashmap keys

### Setup

```bash
cp measurements.txt /tmp
mv measurements.txt measurements.txt.back
ln -s /tmp/measurements.txt measurements.txt
```

### OpenJDK

```bash
java --version
# openjdk 21.0.3 2024-04-16
# OpenJDK Runtime Environment (build 21.0.3+9)
# OpenJDK 64-Bit Server VM (build 21.0.3+9, mixed mode, sharing)
javac src/main/java/dev/morling/onebrc/CalculateAverage_auoie.java -d target/classes
time java -Xmx60G -cp target/classes dev.morling.onebrc.CalculateAverage_auoie > mysolution.txt
```

```
Getting tasks
Got 206 tasks in ms: 59
Finished getting all aggregates in ms: 4721
java -cp target/classes dev.morling.onebrc.CalculateAverage_auoie >   70.27s user 2.54s system 1188% cpu 6.125 total
```

### Graal

```bash
curl -OL https://download.oracle.com/graalvm/21/latest/graalvm-jdk-21_linux-x64_bin.tar.gz
tar xvf graalvm-jdk-21_linux-x64_bin.tar.gz
export PATH=$(pwd)/graalvm-jdk-21.0.3+7.1/bin:$PATH
export JAVA_HOME=$(pwd)/graalvm-jdk-21.0.3+7.1
java --version
# java 21.0.3 2024-04-16 LTS
# Java(TM) SE Runtime Environment Oracle GraalVM 21.0.3+7.1 (build 21.0.3+7-LTS-jvmci-23.1-b37)
# Java HotSpot(TM) 64-Bit Server VM Oracle GraalVM 21.0.3+7.1 (build 21.0.3+7-LTS-jvmci-23.1-b37, mixed mode, sharing)
javac src/main/java/dev/morling/onebrc/CalculateAverage_auoie.java -d target/classes
native-image -cp ./target/classes dev.morling.onebrc.CalculateAverage_auoie
time ./dev.morling.onebrc.calculateaverage_auoie > mysolution.txt
```

```
Getting tasks
Got 206 tasks in ms: 3
Finished getting all aggregates in ms: 5865
./dev.morling.onebrc.calculateaverage_auoie > mysolution.txt  68.78s user 2.22s system 1183% cpu 5.998 total
```
