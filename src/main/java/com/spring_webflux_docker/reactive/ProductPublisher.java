package com.spring_webflux_docker.reactive;

import com.spring_webflux_docker.Product;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class ProductPublisher implements Publisher<Product> {

    private final List<Product> products = List.of(
            new Product("1", "Product-A", 100.0),
            new Product("2", "Product-B", 150.0),
            new Product("3", "Product-C", 200.0),
            new Product("4", "Product-D", 250.0),
            new Product("5", "Product-E", 300.0)
    );

    @Override
    public void subscribe(Subscriber<? super Product> subscriber) {
        subscriber.onSubscribe(new Subscription() {
            private final AtomicBoolean isCanceled = new AtomicBoolean(false);

            @Override
            public void request(long n) {
                Flux.interval(Duration.ofSeconds(2)) // Emit every 2 seconds
                        .map(i -> new Product(
                                String.valueOf(i + 1),
                                "Product-" + (i + 1),
                                100.0 + (i * 10))
                        )
                        .takeWhile(i -> !isCanceled.get()) // Stop if canceled
                        .subscribe(subscriber); // Send to subscriber
            }

            @Override
            public void cancel() {
                isCanceled.set(true); // Handle cancellation
            }
        });
    }

}
