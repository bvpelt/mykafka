# Example Kafka

+ See [producer configuration](http://kafka.apache.org/documentation.html#producerconfigs)
+ See https://github.com/mapr-demos/kafka-sample-programs

## Prerequisits
To start, you need to get Kafka up and running and create some topics.

### Step 1: Download Kafka
Download kafka, the latest version can be found on [apache kafka](http://kafka.apache.org/) the download link

After downloading, goto your destination directory 
 
```
$ tar xzvf ~/Downloads/kafka-0.10.0.0-src.tgz
$ cd kafka-0.10.0.0

```
### Step 2: Start the server
Starting a server means staring zookeeper and kafka!

#### Step 2.1: Start zookeeper
Start a ZooKeeper server. Kafka has a single node Zookeeper configuration built-in.
```
$ bin/zookeeper-server-start.sh config/zookeeper.properties &
```
Note that this will start Zookeeper in the background. To stop
Zookeeper, you will need to bring it back to the foreground and use
control-C or you will need to find the process and kill it.

#### Step 2.2: Start kafka
Now start Kafka itself:
```
$ bin/kafka-server-start.sh config/server.properties &

```
As with Zookeeper, this runs the Kafka broker in the background. To
stop Kafka, you will need to bring it back to the foreground or find
the process and kill it explicitly using `kill`.

### Step 3: Create the topics for the test programs
We need one topic for the test program
```
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic my-topic
```
These can be listed
```
$ bin/kafka-topics.sh --list --zookeeper localhost:2181
my-topic
```
Note that you will see log messages from the Kafka process when you
run Kafka commands. You can switch to a different window if these are
distracting.
The broker can be configured to auto-create new topics as they are mentioned, but that is often considered a bit 
dangerous because mis-spelling a topic name doesn't cause a failure.

## Ready for test
At this point, you should have a working Kafka broker running on your
machine. The next steps are to compile the example programs and play
around with the way that they work.

### Step 4: Compile and package up the example programs
Go back to the directory where you have the example programs and
compile and build the example programs.
```
$ cd ..
$ mvn test
...
```

## Cleaning Up
When you are done playing, stop Kafka and Zookeeper and delete the
data directories they were using from /tmp

```
$ fg
bin/kafka-server-start.sh config/server.properties
^C[2016-02-06 18:06:56,683] INFO [Kafka Server 0], shutting down (kafka.server.KafkaServer)
...
[2016-02-06 18:06:58,977] INFO EventThread shut down (org.apache.zookeeper.ClientCnxn)
[2016-02-06 18:06:58,978] INFO Closed socket connection for client /fe80:0:0:0:0:0:0:1%1:65170 which had sessionid 0x152b958c3300000 (org.apache.zookeeper.server.NIOServerCnxn)
[2016-02-06 18:06:58,979] INFO [Kafka Server 0], shut down completed (kafka.server.KafkaServer)
$ fg
bin/zookeeper-server-start.sh config/zookeeper.properties
^C
$ rm -rf /tmp/zookeeper/  
$ rm -rf /tmp/kafka-logs/
$
```

