package com.example.retryKafka.service;

import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ProcessingService {

    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    @Retry(name = "kafkaRetry", fallbackMethod = "processFallback")
    public void processMessage(String message) {
        AtomicInteger counter = attempts.computeIfAbsent(message, k -> new AtomicInteger(0));
        int attempt = counter.incrementAndGet();
        System.out.println("[ProcessingService] attempt=" + attempt + " for message='" + message + "'");

        if (attempt <= 9) {
            System.out.println("[ProcessingService] Simulating failure for message='" + message + "'");
            throw new RuntimeException("Simulated processing failure");
        }

        System.out.println("[ProcessingService] Successfully processed message='" + message + "' on attempt=" + attempt);
    }

    public void processFallback(String message, Throwable th) {
        System.out.println("[ProcessingService] Fallback invoked for message='" + message + "'. Reason: " + th.getMessage());
    }
}
