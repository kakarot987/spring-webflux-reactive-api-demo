package com.spring_webflux_docker;

import com.spring_webflux_docker.consumer.ProductConsumer;
import com.spring_webflux_docker.producer.ProductProducer;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.http.MediaType;
@RestController
@RequestMapping("/products/stream")
public class ProductController {

    private final ProductProducer productProducer;

    public ProductController(ProductProducer productProducer) {
        this.productProducer = productProducer;
    }

    @GetMapping(value = "/start", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Product> startProductStream() {
        productProducer.startStreaming();
        return productProducer.getProductStream();
    }

    @PostMapping("/stop")
    public String stopProductStream() {
        productProducer.stopStreaming();
        return "Product stream stopped successfully.";
    }
}


