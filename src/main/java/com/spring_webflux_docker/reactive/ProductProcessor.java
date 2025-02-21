package com.spring_webflux_docker.reactive;

import com.spring_webflux_docker.Product;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.stereotype.Component;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Component
public class ProductProcessor implements Processor<Product, Product> {

    private final DirectProcessor<Product> processor;
    private final FluxSink<Product> sink;

    public ProductProcessor() {
        this.processor = DirectProcessor.create();
        this.sink = processor.sink();
    }

    @Override
    public void subscribe(Subscriber<? super Product> subscriber) {
        processor.subscribe(subscriber); // Allow downstream subscribers
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE); // Request all products
    }

    @Override
    public void onNext(Product product) {
        Product discounted = new Product(
                product.getId(),
                product.getName() + " (Discounted)",
                product.getPrice() * 0.9
        );
        sink.next(discounted); // Emit processed product
    }

    @Override
    public void onError(Throwable t) { sink.error(t); }

    @Override
    public void onComplete() { sink.complete(); }

    public Flux<Product> getProcessedProducts() {
        return processor.publish().autoConnect(); // Ensure clients can subscribe anytime
    }
}