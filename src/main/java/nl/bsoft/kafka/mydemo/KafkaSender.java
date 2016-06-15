package nl.bsoft.kafka.mydemo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

public class KafkaSender {
	private static Logger logger = LoggerFactory.getLogger(KafkaSender.class);

	private Producer<String, String> producer = null;

	public KafkaSender() {
		logger.debug("Created a KafkaSender");
	}

	public void startSending() {
		if (producer != null) {
			stopSending();
		}
		createProducer();
		logger.debug("Started producer");
	}

	public void sendMessage(String destination, String key, String message) {
		logger.debug("Send message to: {} key: {} message: {}", destination, key, message);

		ProducerRecord<String, String> pr = new ProducerRecord<String, String>(destination, key, message);

		producer.send(pr);		
	}

	/**
	 * create a producer
	 * 
	 * see http://kafka.apache.org/documentation.html#producerconfigs for valid 
	 * configuration elements
	 * see https://github.com/mapr-demos/kafka-sample-programs/blob/master/src/main/java/com/mapr/examples/Producer.java for example
	 */
	public void createProducer() {
		/*
		 * Properties props = new Properties(); 
		 * props.put("acks", "all");
		 * props.put("auto.commit.interval.ms", 1000); 
		 * //props.put("batch.size",* 16384); 
		 * props.put("block.on.buffer.full", true);
		 * props.put("bootstrap.servers", "localhost:9092");
		 * //props.put("buffer.memory", 33554432); 
		 * //props.put("client.id", * "KafkaSender"); 
		 * //props.put("connections.max.idle.ms", 2000);
		 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 * props.put("linger.ms", 0); 
		 * props.put("retries", 0);
		 * props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer"); 
		 * producer = new KafkaProducer<>(props);
		 */
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		} catch (IOException e) {
			producer = null;
			logger.error("problem loading properties: ", e);
		}

		logger.info("Used parameters to create a producer");
	}

	public void flushProducer() {
		if (producer != null) {
			producer.flush();
		}
		logger.info("Flushed producer");
	}

	public void stopSending() {
		if (producer != null) {
			flushProducer();
			producer.close();
			producer = null;
		}

		logger.info("Stopped producer");
	}

}
