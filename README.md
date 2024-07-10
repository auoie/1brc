# One Billion Row Challenge

Techniques used:

- Map file to memory. Splits the file into approximately 64MB chunks, where each chunk ends in a new line.
- Send memory chunks to a fixed thread pool executor to saturate CPUs
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
Getting buffers
Getting aggregates for 206 buffers
Finished getting all aggregates
java -Xmx60G -cp target/classes dev.morling.onebrc.CalculateAverage_auoie >   72.07s user 25.39s system 1013% cpu 9.615 total
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
Getting buffers
Getting aggregates for 206 buffers
Finished getting all aggregates
./dev.morling.onebrc.calculateaverage_auoie > mysolution.txt  63.02s user 3.34s system 874% cpu 7.587 total
```
