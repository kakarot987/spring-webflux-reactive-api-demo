package com.spring_webflux_docker.producer;

import com.spring_webflux_docker.Product;
import com.spring_webflux_docker.supplier.ProductSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

@Component
public class ProductProducer {

    private static final Logger log = LoggerFactory.getLogger(ProductProducer.class);

    // A single application-wide stream. Slow subscribers skip events instead of
    // causing an unbounded shared buffer or slowing other subscribers.
    private final Sinks.Many<Product> sink = Sinks.many().multicast().directBestEffort();
    private final ProductSupplier productSupplier;
    private final Duration interval;
    private Disposable streamSubscription;

    @Autowired
    public ProductProducer(ProductSupplier productSupplier) {
        this(productSupplier, Duration.ofSeconds(2));
    }

    // Allows fast, deterministic unit tests without changing the production interval.
    ProductProducer(ProductSupplier productSupplier, Duration interval) {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        this.productSupplier = productSupplier;
        this.interval = interval;
    }

    public synchronized void startStreaming() {
        if (streamSubscription != null && !streamSubscription.isDisposed()) {
            return; // Repeated /start requests share the same producer.
        }

        streamSubscription = Flux.interval(interval)
                .map(tick -> productSupplier.supplyProduct().get())
                .subscribe(this::emit, error -> log.error("Product producer stopped due to an error", error));
    }

    public synchronized void stopStreaming() {
        if (streamSubscription != null) {
            streamSubscription.dispose();
            streamSubscription = null;
        }
    }

    public Flux<Product> getProductStream() {
        return sink.asFlux();
    }

    private void emit(Product product) {
        Sinks.EmitResult result = sink.tryEmitNext(product);
        switch (result) {
            case OK:
                break;
            case FAIL_ZERO_SUBSCRIBER:
                // No connected clients; this is a live stream, not event history.
                break;
            case FAIL_OVERFLOW:
                // directBestEffort drops events for subscribers with no demand.
                log.debug("Dropping product because subscribers are not ready");
                break;
            default:
                throw new IllegalStateException("Unable to publish product: " + result);
        }
    }
}
