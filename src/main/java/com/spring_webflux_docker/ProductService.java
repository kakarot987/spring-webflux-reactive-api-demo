package com.spring_webflux_docker;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

@Service
public class ProductService {

    private final ConcurrentHashMap<String, Product> products = new ConcurrentHashMap<>();

    public Flux<Product> getAll() {
        return Flux.fromIterable(products.values());
    }

    public Mono<Product> getById(String id) {
        return Mono.justOrEmpty(products.get(id));
    }

    public Mono<Product> create(Product product) {
        products.put(product.getId(), product);
        return Mono.just(product);
    }

    public Mono<Product> update(String id, Product product) {
        if (products.containsKey(id)) {
            product.setId(id);
            products.put(id, product);
            return Mono.just(product);
        }
        return Mono.empty();
    }

    public Mono<Void> delete(String id) {
        products.remove(id);
        return Mono.empty();
    }
}
