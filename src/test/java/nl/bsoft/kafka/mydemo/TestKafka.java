package nl.bsoft.kafka.mydemo;


import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestKafka {
	private static Logger logger = LoggerFactory.getLogger(TestKafka.class);

	private static int MAX_MESSAGES = 5;
	private static int MAX_PARTITION = 4;
	private static String my_topic = "my-topic";
	private static String my_part_topic = "my-part-topic";

	private KafkaBroker kb = null;
	
	@Rule
	public TestName name = new TestName();

	@Before
	public void startKafka() {
		logger.info("Begin start kafka: {}", name.getMethodName());

		kb = new KafkaBroker();
		KafkaConfig kc = kb.getConfig();
		
		logger.info("advertisedHostName: {}", kc.advertisedHostName());
		logger.info("authorizerClassName: {}", kc.authorizerClassName());
		logger.info("compressionType: {}", kc.compressionType());
		logger.info("hostName: {}", kc.hostName());
		logger.info("interBrokerProtocolVersionString: {}",kc.interBrokerProtocolVersionString());
		logger.info("logCleanupPolicy: {}",kc.logCleanupPolicy());
		logger.info("logMessageFormatVersionString: {}",kc.logMessageFormatVersionString());
		logger.info("saslKerberosKinitCmd: {}", kc.saslKerberosKinitCmd());
		logger.info("saslKerberosServiceName: {}", kc.saslKerberosServiceName());
		logger.info("saslMechanismInterBrokerProtocol: {}", kc.saslMechanismInterBrokerProtocol());
		logger.info("sslClientAuth: {}", kc.sslClientAuth());
		
		
		logger.info("End   start kafka: {}", name.getMethodName());
	}
	
	@After
	public void stopKafka() {
		logger.info("Begin stop kafka");
		kb.stop();
		kb = null;
		logger.info("End   stop kafka");
	}

	public void test_A_Sender() {
		KafkaSender<String,String> kafkaSender = new KafkaSender<String,String>();
		kafkaSender.startSending();

		int i = 0;

		for (i = 0; i < MAX_MESSAGES; i++) {
			logger.info("Sending message: {}", i);			
			String message = Integer.toString(i);
			kafkaSender.sendMessage(my_topic, message);
		}

		kafkaSender.stopSending();
		Assert.assertEquals(MAX_MESSAGES, i);
	}
	
	public void test_B_Receiver() {
		KafkaReceiver<String, String> kafkaReceiver = new KafkaReceiver<String, String>();
		kafkaReceiver.startListening(my_topic);

		kafkaReceiver.getMetrics();
		
		int msgnr_tot = 0;
		int msgnr = 0;

		while (msgnr_tot < MAX_MESSAGES) {
			msgnr = kafkaReceiver.getMessage(1000);
			msgnr_tot += msgnr;
		}

		kafkaReceiver.stopListening();
		Assert.assertEquals(msgnr_tot, MAX_MESSAGES);

	}
	
	@Test
	public void test_value() {
		logger.info("Start test: {}", name.getMethodName());
		
		test_A_Sender();
		
		test_B_Receiver();
		logger.info("End   test: {}", name.getMethodName());
	}
	
	public void test_KVA_Sender() {
		KafkaSender<String, String> kafkaSender = new KafkaSender<String, String>();
		kafkaSender.startSending();

		int i = 0;

		for (i = 0; i < MAX_MESSAGES; i++) {
			logger.info("Sending message: {}", i);
			String key = UUID.randomUUID().toString();
			String message = Integer.toString(i);
			kafkaSender.sendMessage(my_topic, key, message);
		}

		kafkaSender.stopSending();
		Assert.assertEquals(MAX_MESSAGES, i);
	}

	
	public void test_KVB_Receiver() {
		KafkaReceiver<String, String> kafkaReceiver = new KafkaReceiver<String, String>();
		kafkaReceiver.startListening(my_topic);

		kafkaReceiver.getMetrics();
		
		int msgnr_tot = 0;
		int msgnr = 0;

		while (msgnr_tot < MAX_MESSAGES) {
			msgnr = kafkaReceiver.getMessage(1000);
			msgnr_tot += msgnr;
		}

		kafkaReceiver.stopListening();
		Assert.assertEquals(msgnr_tot, MAX_MESSAGES);

	}
	
	@Test
	public void test_key_value() {
		logger.info("Start test: {}", name.getMethodName());
		
		test_KVA_Sender();
		
		test_KVB_Receiver();
		logger.info("End   test: {}", name.getMethodName());
	}
	
	public void test_partitionA_Sender() {
		KafkaSender<String, String> kafkaSender = new KafkaSender<String, String>();
		kafkaSender.startSending();

		int i = 0;

		for (i = 0; i < MAX_MESSAGES; i++) {
			logger.info("Sending message: {}", i);
			String key = UUID.randomUUID().toString();
			String message = Integer.toString(i);
			kafkaSender.sendMessage(my_part_topic, (i % MAX_PARTITION), key, message);
		}

		kafkaSender.stopSending();
		Assert.assertEquals(MAX_MESSAGES, i);
	}

	public void test_partitionB_Receiver() {
		KafkaReceiver<String, String> kafkaReceiver = new KafkaReceiver<String, String>();
		kafkaReceiver.startListening(my_part_topic);

		kafkaReceiver.getMetrics();
		
		int msgnr_tot = 0;
		int msgnr = 0;

		while (msgnr_tot < MAX_MESSAGES) {
			msgnr = kafkaReceiver.getMessage(1000);
			msgnr_tot += msgnr;
		}

		kafkaReceiver.stopListening();
		Assert.assertEquals(msgnr_tot, MAX_MESSAGES);

	}
	
	/*
	@Test
	public void test_partition_value() {
		logger.info("Start test: {}", name.getMethodName());
		
		test_partitionA_Sender();
		
		test_partitionB_Receiver();
		logger.info("End   test: {}", name.getMethodName());
	}
	*/	
}
