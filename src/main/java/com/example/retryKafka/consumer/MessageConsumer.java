package com.example.retryKafka.consumer;

import com.example.retryKafka.service.ProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private final ProcessingService processingService;

    public MessageConsumer(ProcessingService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(topics = "test-topic", groupId = "retry-group")
    public void listen(ConsumerRecord<String, String> record) {
        String message = record.value();
        System.out.println("[Consumer] Received message: " + message);
        processingService.processMessage(message);
    }
}
