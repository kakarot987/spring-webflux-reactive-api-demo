package com.spring_webflux_docker.supplier;

import com.spring_webflux_docker.Product;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

@Component
public class ProductSupplier {

    public Supplier<Product> supplyProduct() {
        return () -> new Product(
                UUID.randomUUID().toString(),
                "Product-" + ThreadLocalRandom.current().nextInt(1, 100),
                ThreadLocalRandom.current().nextDouble(10.0, 500.0)
        );
    }
}

