package com.algaworks.algashop.ordering.infrastructure.adapters.out.messaging.kafka.saga;

import com.algaworks.algashop.ordering.core.application.CommandPublishingException;
import com.algaworks.algashop.ordering.core.application.OutboundIntegrationCommand;
import com.algaworks.algashop.ordering.core.application.invoice.command.CancelInvoiceIntegrationCommand;
import com.algaworks.algashop.ordering.core.application.invoice.command.IssueInvoiceIntegrationCommand;
import com.algaworks.algashop.ordering.core.application.stock.command.ReserveStockIntegrationCommand;
import com.algaworks.algashop.ordering.infrastructure.adapters.out.messaging.saga.PlaceOrderSagaCommandTopics;
import com.algaworks.algashop.ordering.infrastructure.config.kafka.AlgaShopMessagingKafkaProperties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

class KafkaPlaceOrderSagaIntegrationCommandPublisherTest {

	private static final String BILLING_TOPIC = "billing.invoice.commands";
	private static final String STOCK_TOPIC = "product-catalog.stock.commands";
	private static final String REPLIES_TOPIC = "ordering.saga.replies";
	private static final String ORDER_ID = "01K1Z5T7Q4XK1B9Q2H3J4K5M6N";
	private static final String CORRELATION_ID = UUID.randomUUID().toString();

	@SuppressWarnings("unchecked")
	private final KafkaTemplate<String, Object> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
	private KafkaPlaceOrderSagaIntegrationCommandPublisher publisher;

	@BeforeEach
	void setUp() {
		AlgaShopMessagingKafkaProperties properties = new AlgaShopMessagingKafkaProperties();
		properties.setBillingInvoiceCommandsTopicName(BILLING_TOPIC);
		properties.setProductCatalogStockCommandsTopicName(STOCK_TOPIC);
		properties.setSagaRepliesTopicName(REPLIES_TOPIC);
		publisher = new KafkaPlaceOrderSagaIntegrationCommandPublisher(
				kafkaTemplate, new PlaceOrderSagaCommandTopics(properties), properties);
	}

	@Test
	void shouldSendIssueInvoiceToBillingTopicKeyedByOrderId() {
		givenKafkaAcknowledges();
		IssueInvoiceIntegrationCommand command = new IssueInvoiceIntegrationCommand();
		command.setOrderId(ORDER_ID);

		publisher.send(CORRELATION_ID, command);

		ProducerRecord<String, Object> record = sentRecord();
		Assertions.assertThat(record.topic()).isEqualTo(BILLING_TOPIC);
		Assertions.assertThat(record.key()).isEqualTo(ORDER_ID);
		Assertions.assertThat(record.value()).isSameAs(command);
		Assertions.assertThat(headerOf(record, KafkaHeaders.CORRELATION_ID)).isEqualTo(CORRELATION_ID);
		Assertions.assertThat(headerOf(record, KafkaHeaders.REPLY_TOPIC)).isEqualTo(REPLIES_TOPIC);
	}

	@Test
	void shouldSendCancelInvoiceToBillingTopicKeyedByOrderId() {
		givenKafkaAcknowledges();
		CancelInvoiceIntegrationCommand command = new CancelInvoiceIntegrationCommand(ORDER_ID);

		publisher.send(CORRELATION_ID, command);

		ProducerRecord<String, Object> record = sentRecord();
		Assertions.assertThat(record.topic()).isEqualTo(BILLING_TOPIC);
		Assertions.assertThat(record.key()).isEqualTo(ORDER_ID);
		Assertions.assertThat(record.value()).isSameAs(command);
		Assertions.assertThat(headerOf(record, KafkaHeaders.CORRELATION_ID)).isEqualTo(CORRELATION_ID);
		Assertions.assertThat(headerOf(record, KafkaHeaders.REPLY_TOPIC)).isEqualTo(REPLIES_TOPIC);
	}

	@Test
	void shouldSendReserveStockToStockTopicKeyedByOrderId() {
		givenKafkaAcknowledges();
		ReserveStockIntegrationCommand command = new ReserveStockIntegrationCommand(ORDER_ID,
				List.of(new ReserveStockIntegrationCommand.Item(UUID.randomUUID(), 2)));

		publisher.send(CORRELATION_ID, command);

		ProducerRecord<String, Object> record = sentRecord();
		Assertions.assertThat(record.topic()).isEqualTo(STOCK_TOPIC);
		Assertions.assertThat(record.key()).isEqualTo(ORDER_ID);
		Assertions.assertThat(record.value()).isSameAs(command);
		Assertions.assertThat(headerOf(record, KafkaHeaders.CORRELATION_ID)).isEqualTo(CORRELATION_ID);
		Assertions.assertThat(headerOf(record, KafkaHeaders.REPLY_TOPIC)).isEqualTo(REPLIES_TOPIC);
	}

	@Test
	void shouldFailWhenKafkaDoesNotAcknowledge() {
		Mockito.when(kafkaTemplate.send(Mockito.<ProducerRecord<String, Object>>any()))
				.thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));
		IssueInvoiceIntegrationCommand command = new IssueInvoiceIntegrationCommand();
		command.setOrderId(ORDER_ID);

		Assertions.assertThatExceptionOfType(CommandPublishingException.class)
				.isThrownBy(() -> publisher.send(CORRELATION_ID, command));
	}

	@Test
	void shouldRejectUnknownCommandWithoutSending() {
		OutboundIntegrationCommand unknown = () -> ORDER_ID;

		Assertions.assertThatIllegalArgumentException()
				.isThrownBy(() -> publisher.send(CORRELATION_ID, unknown));
		Mockito.verifyNoInteractions(kafkaTemplate);
	}

	private void givenKafkaAcknowledges() {
		RecordMetadata metadata = new RecordMetadata(new TopicPartition(BILLING_TOPIC, 0), 0L, 0, 0L, 0, 0);
		Mockito.when(kafkaTemplate.send(Mockito.<ProducerRecord<String, Object>>any()))
				.thenAnswer(invocation -> CompletableFuture.completedFuture(
						new SendResult<>(invocation.getArgument(0), metadata)));
	}

	private ProducerRecord<String, Object> sentRecord() {
		@SuppressWarnings("unchecked")
		ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		Mockito.verify(kafkaTemplate).send(captor.capture());
		return captor.getValue();
	}

	private String headerOf(ProducerRecord<String, Object> record, String name) {
		return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
	}
}
