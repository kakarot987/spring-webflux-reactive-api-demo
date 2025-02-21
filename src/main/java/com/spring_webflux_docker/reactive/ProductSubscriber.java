package com.spring_webflux_docker.reactive;

import com.spring_webflux_docker.Product;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Component
public class ProductSubscriber implements Subscriber<Product> {

    private Subscription subscription;

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1); // Request the first product
    }

    @Override
    public void onNext(Product product) {
        System.out.println("Received: " + product.getName() + " - $" + product.getPrice());
        subscription.request(1); // Request next product after processing
    }

    @Override
    public void onError(Throwable t) {
        System.err.println(" Error: " + t.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println(" All products processed.");
    }
}

