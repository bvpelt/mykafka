package nl.bsoft.kafka.mykafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

public class KafkaSender<K, V> {
	private static Logger logger = LoggerFactory.getLogger(KafkaSender.class);

	private Producer<K, V> producer = null;

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

	public void sendMessage(String topic, Integer partition, K key, V message) {
		logger.debug("Send message topic: {} partition: {} key: {} message: {}", topic, partition, key, message);

		ProducerRecord<K, V> pr = new ProducerRecord<K, V>(topic, partition, key, message);

		producer.send(pr);
	}

	public void sendMessage(String topic, Integer partition, Long timestamp, K key, V message) {
		logger.debug("Send message topic: {} partition: {} timestamp: {} key: {} message: {}", topic, partition,
				timestamp, key, message);

		ProducerRecord<K, V> pr = new ProducerRecord<K, V>(topic, partition, timestamp, key, message);

		producer.send(pr);
	}

	public void sendMessage(String topic, K key, V message) {
		logger.debug("Send message topic: {} key: {} message: {}", topic, key, message);

		ProducerRecord<K, V> pr = new ProducerRecord<K, V>(topic, key, message);

		producer.send(pr);
	}

	public void sendMessage(String topic, V message) {
		logger.debug("Send message topic: {} key: {} message: {}", topic, message);

		ProducerRecord<K, V> pr = new ProducerRecord<K, V>(topic, message);

		producer.send(pr);
	}

	/**
	 * create a producer
	 * 
	 * see http://kafka.apache.org/documentation.html#producerconfigs for valid
	 * configuration elements see
	 * https://github.com/mapr-demos/kafka-sample-programs/blob/master/src/main/
	 * java/com/mapr/examples/Producer.java for example
	 */
	private void createProducer() {
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			SortedProperties properties = new SortedProperties();
			properties.load(props);

			if (logger.isDebugEnabled()) {
				logger.debug("Start List of Producer specified properties");
				for (Enumeration<Object> e = properties.keys(); e.hasMoreElements();) {
					Object o = e.nextElement();
					logger.debug("Property: {} Value: {}", o, properties.get(o));
				}
				logger.debug("End   List of Producer specified properties");
			}

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
