package com.spring_webflux_docker;

import com.spring_webflux_docker.consumer.ProductConsumer;
import com.spring_webflux_docker.producer.ProductProducer;
import com.spring_webflux_docker.reactive.ProductProcessor;
import com.spring_webflux_docker.reactive.ProductPublisher;
import com.spring_webflux_docker.reactive.ProductSubscriber;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.http.MediaType;
@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductProducer productProducer;

    private final ProductPublisher publisher;
    private final ProductProcessor processor;
    private final ProductSubscriber subscriber;
    public ProductController(ProductProducer productProducer,ProductPublisher publisher, ProductProcessor processor, ProductSubscriber subscriber) {
        this.productProducer = productProducer;
        this.publisher = publisher;
        this.processor = processor;
        this.subscriber = subscriber;

        publisher.subscribe(processor);
        processor.subscribe(subscriber);
    }

    @GetMapping(value = "/stream/start", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Product> startProductStream() {
        productProducer.startStreaming();
        return productProducer.getProductStream();
    }

    @PostMapping("/stream/stop")
    public String stopProductStream() {
        productProducer.stopStreaming();
        return "Product stream stopped successfully.";
    }

    @GetMapping("/reactive-stream/start")
    public String startReactiveProductStream() {
        processor.onNext(new Product("100", "Live-Product", 500.0)); // Trigger one product immediately
        return "Product stream started. Connect to /products/reactive-stream to receive data.";
    }

    @GetMapping(value = "/reactive-stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Product> streamProducts() {
        return processor.getProcessedProducts();
    }
}


