# Spring WebFlux reactive streaming demo

A focused **Spring Boot 3.4.2 + WebFlux + Project Reactor** example of live product events using Server-Sent Events (SSE). This project deliberately uses **WebFlux only** (no Spring MVC starter), with Java 21 LTS.

## Run

Requires JDK 21. Run `./gradlew bootRun`.

In two terminals:

```sh
curl -N -H "Accept: text/event-stream" http://localhost:8080/products/stream/start
curl -X POST http://localhost:8080/products/stream/stop
```

Calling `GET /products/stream/start` starts the **one application-wide product producer** and subscribes the caller to its live events. Additional clients share the same producer. A disconnect only removes that SSE subscriber. `POST /products/stream/stop` globally stops generating events (existing SSE connections remain open); another start request restarts production. It is **not** a per-user stop endpoint.

The stream uses Reactor `Sinks.many().multicast().directBestEffort()`. It does not replay past events, and events may be dropped for a subscriber that cannot keep up; no unbounded shared event queue is retained. Requests to start/stop are synchronized to prevent racing timers.

## Test

```sh
./gradlew test
```

Tests cover SSE endpoint contracts, concurrent start calls, restarting, and slow subscribers. The intentionally unrelated in-memory CRUD service and console consumer were removed so this stays a concise streaming demo.
