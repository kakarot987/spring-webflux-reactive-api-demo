package com.spring_webflux_docker;

import com.spring_webflux_docker.producer.ProductProducer;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/products/stream")
public class ProductController {

    private final ProductProducer productProducer;

    public ProductController(ProductProducer productProducer) {
        this.productProducer = productProducer;
    }

    // Starting production is application-wide. Connecting a second client does
    // not create a second timer; disconnecting one client does not stop it.
    @GetMapping(value = "/start", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Product> startProductStream() {
        productProducer.startStreaming();
        return productProducer.getProductStream();
    }

    // Global stop: connected SSE clients remain open, but no new events arrive.
    @PostMapping("/stop")
    public String stopProductStream() {
        productProducer.stopStreaming();
        return "Product stream stopped successfully.";
    }
}
