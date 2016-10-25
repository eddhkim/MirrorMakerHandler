package org.apache.kafka.custom;

import kafka.consumer.BaseConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

@Test
public class ReplicationMessageHandlerTest {

	private ReplicationMessageHandler handler;

	@BeforeClass
	public void beforeClass() {
		handler = new ReplicationMessageHandler();
	}

	@Test
	public void handler() {
		final String topic = "HELLO-WORLD";
		final Integer partition = 123;
		final Long offset = 5678L;
		final Long current = System.currentTimeMillis();
		final String key = "Key1";
		final String value = "Value1";
		BaseConsumerRecord record = new BaseConsumerRecord(topic, partition, offset, current, TimestampType.CREATE_TIME,
				key.getBytes(), value.getBytes());

		// run
		List<ProducerRecord<byte[], byte[]>> records = handler.handle(record);

		// verify
		Assert.assertEquals(records.size(), 1);
		Assert.assertEquals(records.get(0).topic(), "REP-HELLO-WORLD");
		Assert.assertEquals(records.get(0).partition(), partition);
		Assert.assertEquals(records.get(0).timestamp(), current);
		Assert.assertEquals(records.get(0).key(), key.getBytes());
		Assert.assertEquals(records.get(0).value(), value.getBytes());
	}

	@Test
	public void handler_noKeyValue() {
		final String topic = "HELLO-WORLD";
		final Integer partition = 123;
		final Long offset = 5678L;
		final Long current = System.currentTimeMillis();
		BaseConsumerRecord record = new BaseConsumerRecord(topic, partition, offset, current, TimestampType.CREATE_TIME,
				null, null);

		// run
		List<ProducerRecord<byte[], byte[]>> records = handler.handle(record);

		// verify
		Assert.assertEquals(records.size(), 1);
		Assert.assertEquals(records.get(0).topic(), "REP-HELLO-WORLD");
		Assert.assertEquals(records.get(0).partition(), partition);
		Assert.assertEquals(records.get(0).timestamp(), current);
		Assert.assertEquals(records.get(0).key(), null);
		Assert.assertEquals(records.get(0).value(), null);
	}
}
