package org.apache.kafka.custom;

import kafka.consumer.BaseConsumerRecord;
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;

public class ReplicationMessageHandler implements MirrorMaker.MirrorMakerMessageHandler {

	private static final String PREFIX = "REP-";

	public List<ProducerRecord<byte[], byte[]>> handle(BaseConsumerRecord record) {
		return Collections.singletonList(new ProducerRecord<byte[], byte[]>(PREFIX + record.topic(), record.partition(),
				record.timestamp() < 0 ? System.currentTimeMillis() : record.timestamp(), record.key(), record.value()));
	}
}
