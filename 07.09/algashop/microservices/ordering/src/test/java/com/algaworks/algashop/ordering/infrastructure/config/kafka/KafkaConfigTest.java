package com.algaworks.algashop.ordering.infrastructure.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConfigTest {

	private final KafkaConfig kafkaConfig = new KafkaConfig();

	@Test
	void shouldCreateDeadLetterTopicForInvoiceEvents() {
		AlgaShopMessagingKafkaProperties properties = new AlgaShopMessagingKafkaProperties();
		properties.setInvoiceEventTopicName("billing.invoice.events");

		NewTopic topic = kafkaConfig.invoiceEventsDlt(properties);

		assertThat(topic.name()).isEqualTo("ordering.dlt.billing.invoice.events");
		assertThat(topic.numPartitions()).isEqualTo(3);
		assertThat(topic.replicationFactor()).isEqualTo((short) 3);
		assertThat(topic.configs())
				.containsEntry("min.insync.replicas", "2")
				.containsEntry("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1_000));
	}
}
