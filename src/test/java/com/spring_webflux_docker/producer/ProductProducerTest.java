package com.spring_webflux_docker.producer;

import com.spring_webflux_docker.Product;
import com.spring_webflux_docker.supplier.ProductSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProductProducerTest {

    private ProductProducer producer;
    private AtomicInteger generated;

    @BeforeEach
    void setUp() {
        generated = new AtomicInteger();
        ProductSupplier supplier = mock(ProductSupplier.class);
        when(supplier.supplyProduct()).thenReturn(() ->
                new Product(String.valueOf(generated.incrementAndGet()), "Sample", 10.0));
        producer = new ProductProducer(supplier, Duration.ofMillis(25));
    }

    @AfterEach
    void tearDown() {
        producer.stopStreaming();
    }

    @Test
    void streamsToSubscribersAndCanRestartAfterStopping() {
        StepVerifier.create(producer.getProductStream().take(2))
                .then(producer::startStreaming)
                .expectNextMatches(product -> product.getName().equals("Sample"))
                .expectNextCount(1)
                .verify(Duration.ofSeconds(5));

        producer.stopStreaming();

        StepVerifier.create(producer.getProductStream().take(1))
                .then(producer::startStreaming)
                .expectNextCount(1)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void concurrentStartRequestsAreSafeAndDoNotBreakTheStream() {
        StepVerifier.create(producer.getProductStream().take(2))
                .then(() -> IntStream.range(0, 20).parallel().forEach(i -> producer.startStreaming()))
                .expectNextCount(2)
                .verify(Duration.ofSeconds(5));
        assertTrue(generated.get() >= 2);
    }

    @Test
    void subscriberWithoutDemandDoesNotBlockAnotherSubscriber() {
        BaseSubscriber<Product> slow = new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                // Deliberately do not request: the stream must not buffer forever.
            }
        };
        producer.getProductStream().subscribe(slow);

        try {
            StepVerifier.create(producer.getProductStream().take(2))
                    .then(producer::startStreaming)
                    .expectNextCount(2)
                    .verify(Duration.ofSeconds(5));
        } finally {
            slow.dispose();
        }
    }

    @Test
    void simultaneousSubscribersReceiveTheSameLiveEvent() {
        // Both subscribers attach before the application-wide producer starts.
        Mono<Product> first = producer.getProductStream().next();
        Mono<Product> second = producer.getProductStream().next();

        StepVerifier.create(Mono.zip(first, second))
                .then(producer::startStreaming)
                .assertNext(pair -> assertEquals(pair.getT1().getId(), pair.getT2().getId()))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void cancellingOneClientLeavesOtherClientsAndProducerRunning() {
        AtomicInteger cancellations = new AtomicInteger();
        Mono<Product> disconnectingClient = producer.getProductStream()
                .doOnCancel(cancellations::incrementAndGet)
                .next();
        Mono<java.util.List<Product>> continuingClient = producer.getProductStream()
                .take(3)
                .collectList();

        StepVerifier.create(Mono.zip(disconnectingClient, continuingClient))
                .then(producer::startStreaming)
                .assertNext(pair -> {
                    assertEquals(3, pair.getT2().size(), "Remaining client keeps getting events");
                    assertEquals(pair.getT1().getId(), pair.getT2().get(0).getId());
                    assertEquals(1, cancellations.get(), "First client cancelled its subscription");
                })
                .verify(Duration.ofSeconds(5));
    }
}
