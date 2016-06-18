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
import kafka.server.KafkaServerStartable;

public class KafkaBroker {

	private static Logger logger = LoggerFactory.getLogger(KafkaBroker.class);

	private TestingServer zkServer = null;

	public KafkaConfig kafkaConfig = null;

	public KafkaServerStartable kafkaServer = null;

	/**
	 * default constructor
	 */
	public KafkaBroker() {
		this(createProperties());
	}

	public KafkaBroker(Properties properties) {
		try {
			// Create and run zookeeper
			zkServer = new TestingServer(2181, true);

			// add zookeeper connect string to the properties before setting
			// kafkaconfig
			properties.put("zookeeper.connect", zkServer.getConnectString());
			kafkaConfig = new KafkaConfig(properties);

			// create and start kafka with kafkaconfig
			kafkaServer = new KafkaServerStartable(kafkaConfig);
			kafkaServer.startup();
		} catch (Exception e) {
			logger.error("Problem starting broker: ", e);
		}

		logger.info("Embedded kafka is up");
	}

	private static SortedProperties createProperties() {
		SortedProperties properties = null;
		try (InputStream props = Resources.getResource("kafkaBroker.properties").openStream()) {
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

	/**
	 * Stop of the broker means - stop kafkaServer - stop zookeeper
	 */
	public void stop() {

		if (kafkaServer != null) {
			kafkaServer.shutdown();
		}

		try {
			if (zkServer != null) {
				zkServer.stop();
			}
		} catch (IOException e) {
			logger.error("Error shutdown kafka: ", e);
		} finally {
			kafkaServer = null;
			zkServer = null;
		}

		logger.info("embedded kafka stop");
	}

	public KafkaConfig getConfig() {
		KafkaConfig kc = null;

		if (kafkaServer != null) {
			kc = kafkaServer.serverConfig();
		}
		return kc;
	}

}
