package nl.bsoft.kafka.mydemo;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestKafka {
	private static Logger logger = LoggerFactory.getLogger(TestKafka.class);

	private static int MAX_MESSAGES = 5;
	private static String topic = "my-topic";

	@Rule
	public TestName name = new TestName();

	@Test
	public void test_A_Sender() {
		logger.info("Start test: {}", name.getMethodName());
		KafkaSender kafkaSender = new KafkaSender();
		kafkaSender.startSending();

		int i = 0;

		for (i = 0; i < MAX_MESSAGES; i++) {
			logger.info("Sending message: {}", i);
			String key = Integer.toString(i);
			String message = Integer.toString(i);
			kafkaSender.sendMessage(topic, key, message);
		}

		kafkaSender.stopSending();
		Assert.assertEquals(MAX_MESSAGES, i);

	}

	@Test
	public void test_B_Receiver() {
		logger.info("Start test: {}", name.getMethodName());

		KafkaReceiver kafkaReceiver = new KafkaReceiver();
		kafkaReceiver.startListening(topic);

		int msgnr_tot = 0;

		int msgnr = 0;

		while (msgnr_tot < MAX_MESSAGES) {
			msgnr = kafkaReceiver.getMessage(1000);
			msgnr_tot += msgnr;
		}

		kafkaReceiver.stopListening();
		Assert.assertEquals(msgnr_tot, MAX_MESSAGES);

	}

}
