package nl.bsoft.kafka.mydemo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

public class KafkaReceiver {
	private static Logger logger = LoggerFactory.getLogger(KafkaReceiver.class);

	private KafkaConsumer<String, String> consumer = null;

	public KafkaReceiver() {
		logger.debug("Created a KafkaReceiver");
	}

	public void createConsumer(String... topics) {

		try (InputStream props = Resources.getResource("consumer.props").openStream()) {
			SortedProperties properties = new SortedProperties();
			properties.load(props);
			if (properties.getProperty("group.id") == null) {
				properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
			}

			if (logger.isDebugEnabled()) {
				logger.debug("Start List of Consumer specified properties");
				for (Enumeration<Object> e = properties.keys(); e.hasMoreElements();) {
					Object o = e.nextElement();
					logger.debug("Property: {} Value: {}", o, properties.get(o));
				}
				logger.debug("End   List of Consumer specified properties");
			}

			consumer = new KafkaConsumer<>(properties);

			consumer.subscribe(Arrays.asList(topics));

		} catch (IOException e) {
			consumer = null;
			logger.error("Problem reading configuration for client: ", e);
		}

		StringBuffer sb = new StringBuffer();
		for (String t : topics) {
			sb.append(t);
			sb.append(", ");
		}
		logger.info("Used parameters to create a consumer for: {}", sb.toString());
	}

	public void startListening(String... topics) {
		if (consumer != null) {
			stopListening();
		}
		createConsumer(topics);
		logger.debug("Started consumer");
	}

	public void stopListening() {
		if (consumer != null) {
			consumer.close();
			consumer = null;
		}
		logger.debug("Stopped consumer");
	}

	public int getMessage(int timeout) {
		logger.debug("Start reading messages");

		boolean goOn = true;
		int msgnr = 0;

		while (goOn) {
			ConsumerRecords<String, String> records = consumer.poll(timeout);
			goOn = (records.count() > 0);
			if (goOn) {
				for (ConsumerRecord<String, String> record : records) {
					msgnr++;
					logger.info("Received offset: {} key: {} value: {}", record.offset(), record.key(), record.value());
				}
			}
		}

		logger.debug("End reading messages, found: {} messages", msgnr);
		return msgnr;
	}
}
