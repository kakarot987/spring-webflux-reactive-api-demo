package com.spring_webflux_docker.producer;

import com.spring_webflux_docker.Product;
import com.spring_webflux_docker.supplier.ProductSupplier;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

@Component
public class ProductProducer {

    private final ProductSupplier productSupplier;
    private final Sinks.Many<Product> sink = Sinks.many().multicast().onBackpressureBuffer();
    private Disposable streamSubscription;

    public ProductProducer(ProductSupplier productSupplier) {
        this.productSupplier = productSupplier;
    }

    public void startStreaming() {
        if (streamSubscription == null || streamSubscription.isDisposed()) {
            streamSubscription = Flux.interval(Duration.ofSeconds(2))
                    .map(tick -> productSupplier.supplyProduct().get())
                    .doOnNext(sink::tryEmitNext)
                    .subscribe();
        }
    }

    public void stopStreaming() {
        if (streamSubscription != null && !streamSubscription.isDisposed()) {
            streamSubscription.dispose();
        }
    }

    public Flux<Product> getProductStream() {
        return sink.asFlux();
    }
}

