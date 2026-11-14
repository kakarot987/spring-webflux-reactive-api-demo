package com.spring_webflux_docker;

import com.spring_webflux_docker.producer.ProductProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@WebFluxTest(ProductController.class)
class ProductControllerTest {

    @Autowired
    private WebTestClient client;

    @MockBean
    private ProductProducer producer;

    @Test
    void startReturnsEventStream() {
        when(producer.getProductStream()).thenReturn(Flux.just(new Product("1", "Sample", 10.0)));

        client.get().uri("/products/stream/start")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .expectBody()
                .consumeWith(response -> {});

        verify(producer).startStreaming();
    }

    @Test
    void stopRequiresPostAndStopsGlobalProducer() {
        client.post().uri("/products/stream/stop")
                .exchange()
                .expectStatus().isOk();
        verify(producer).stopStreaming();

        client.get().uri("/products/stream/stop")
                .exchange()
                .expectStatus().is4xxClientError();
    }
}
