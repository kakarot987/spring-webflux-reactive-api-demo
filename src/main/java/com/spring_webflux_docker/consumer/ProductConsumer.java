package com.spring_webflux_docker.consumer;

import com.spring_webflux_docker.Product;
import org.springframework.stereotype.Component;

@Component
public class ProductConsumer {

    public void consume(Product product) {
        System.out.println("Consumed Product: " + product.getName() + " - Price: $" + product.getPrice());
    }
}

