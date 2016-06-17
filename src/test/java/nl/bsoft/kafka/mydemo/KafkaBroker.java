package nl.bsoft.kafka.mydemo;

import java.io.IOException;
import java.io.InputStream;

import java.util.Enumeration;
import java.util.Properties;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;


public class KafkaBroker {
	private static Logger logger = LoggerFactory.getLogger(KafkaBroker.class);

	private TestingServer zkServer;
	/*
	 * // location of kafka logging file: public static final String
	 * DEFAULT_LOG_DIR = "/tmp/embedded/kafka/"; public static final String
	 * LOCALHOST_BROKER = "0:localhost:9092"; public static final String
	 * TEST_TOPIC = "test-topic";
	 */
	public KafkaConfig kafkaConfig;
	// This is the actual Kafka server:
	public KafkaServer kafkaServer;

	/**
	 * default constructor
	 */

	public KafkaBroker(Properties properties) {
		try {
			// Create and run zookeeper
			zkServer = new TestingServer(2181, true);
			properties.put("zookeeper.connect", zkServer.getConnectString());
			kafkaConfig = new KafkaConfig(properties);

			// TODO: Try using kafka.server.KafkaServerStartable instead. see https://www.mail-archive.com/dev@kafka.apache.org/msg51969.html 
			
			kafkaServer = new KafkaServer(kafkaConfig, new TestTime(), null);
			kafkaServer.startup();
		} catch (Exception e) {
			logger.error("Problem starting broker: ", e);
		}

		logger.info("embedded kafka is up");
	}

	public KafkaBroker() {
		this(createProperties());
	}

	private static SortedProperties createProperties() {
		SortedProperties properties = null;
		try (InputStream props = Resources.getResource("kafka.properties").openStream()) {
			properties = new SortedProperties();
			properties.load(props);

			if (logger.isDebugEnabled()) {
				logger.debug("Start List of Producer specified properties");
				for (Enumeration<Object> e = properties.keys(); e.hasMoreElements();) {
					Object o = e.nextElement();
					logger.debug("Property: {} Value: {}", o, properties.get(o));
				}
				logger.debug("End   List of Producer specified properties");
			}
		} catch (IOException e) {
			logger.error("problem loading properties: ", e);
		}
		return properties;
	}

	public void stop() {
		kafkaServer.shutdown();
		try {
			zkServer.stop();
		} catch (IOException e) {
			logger.error("Error shutdown kafka: ", e);
		}
		logger.info("embedded kafka stop");
	}

	public KafkaConfig getConfig() {
		return kafkaServer.config();
	}
	
}
