# Lab 08 · Kafka Streams

!!! info "Learning Goal"
    Build an **order enrichment + aggregation pipeline** using Kafka Streams. You'll implement:
    - Stream-Table join (enrich orders with customer data)
    - Stateful aggregation (count orders per customer per minute)
    - Interactive queries (REST endpoint to query order counts)

**Estimated time:** 40 minutes  
**Prerequisites:** 
- Docker & Kafka running (`docker-compose up`)
- Lab 01–07 completed (or at least familiar with topics/producers)

---

## Architecture Overview

```
Orders Topic
    ↓
┌─────────────────────────────────┐
│  Kafka Streams Topology         │
├─────────────────────────────────┤
│ 1. KStream (orders)             │
│ 2. KTable (customers)           │
│ 3. Join: enrich order           │
│ 4. Aggregate: count per minute  │
│ 5. Interactive query store      │
└─────────────────────────────────┘
    ↓                    ↓
Enriched Orders     Order Counts
    Topic              Topic
```

---

## Step 0: Verify Prerequisites

Ensure Kafka, Zookeeper, and Schema Registry are running:

```bash
cd /Users/sgovinda/Learn/Kafka-Learning
docker-compose up -d

# Check containers
docker ps | grep -E "kafka|zookeeper|registry"
```

Expected output: 3+ containers running.

---

## Step 1: Create Required Topics

Create topics for orders, customers, enriched orders, and order counts:

```bash
# Orders topic
docker exec -it kafka kafka-topics.sh \
  --create \
  --topic orders-topic \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

# Customers topic (will be used as KTable)
docker exec -it kafka kafka-topics.sh \
  --create \
  --topic customers-topic \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

# Output topic — enriched orders
docker exec -it kafka kafka-topics.sh \
  --create \
  --topic enriched-orders-topic \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

# Output topic — order counts
docker exec -it kafka kafka-topics.sh \
  --create \
  --topic order-counts-topic \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

# Verify
docker exec -it kafka kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092 | grep -E "orders|customers|enriched|counts"
```

Expected output:
```
customers-topic
enriched-orders-topic
order-counts-topic
orders-topic
```

---

## Step 2: Create Domain Classes

Add the Order and Customer classes to your project:

**File:** `src/main/java/com/example/kafka/streams/domain/Order.java`

```java
package com.example.kafka.streams.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    @JsonProperty("order_id")
    private String orderId;
    
    @JsonProperty("customer_id")
    private String customerId;
    
    @JsonProperty("amount")
    private BigDecimal amount;
    
    @JsonProperty("timestamp")
    private long timestamp;
}
```

**File:** `src/main/java/com/example/kafka/streams/domain/Customer.java`

```java
package com.example.kafka.streams.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer {
    @JsonProperty("customer_id")
    private String customerId;
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("email")
    private String email;
    
    @JsonProperty("tier")
    private String tier; // "GOLD", "SILVER", "BRONZE"
}
```

**File:** `src/main/java/com/example/kafka/streams/domain/EnrichedOrder.java`

```java
package com.example.kafka.streams.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedOrder {
    @JsonProperty("order_id")
    private String orderId;
    
    @JsonProperty("customer_id")
    private String customerId;
    
    @JsonProperty("customer_name")
    private String customerName;
    
    @JsonProperty("customer_tier")
    private String customerTier;
    
    @JsonProperty("amount")
    private BigDecimal amount;
    
    @JsonProperty("timestamp")
    private long timestamp;
}
```

**File:** `src/main/java/com/example/kafka/streams/domain/OrderStats.java`

```java
package com.example.kafka.streams.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderStats {
    @JsonProperty("customer_id")
    private String customerId;
    
    @JsonProperty("count")
    private long count;
    
    @JsonProperty("window_start")
    private long windowStart;
    
    @JsonProperty("window_end")
    private long windowEnd;
}
```

---

## Step 3: Configure Kafka Streams

**File:** `src/main/java/com/example/kafka/streams/config/StreamsConfig.java`

```java
package com.example.kafka.streams.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.example.kafka.streams.domain.Order;
import com.example.kafka.streams.domain.Customer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfiguration {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.application.name:streams-app}")
    private String applicationId;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        
        // Use at-least-once for this lab (simpler for debugging)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, 
                  StreamsConfig.AT_LEAST_ONCE_CONFIG);
        
        // State store cleanup
        props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 10000L);
        
        // Default serdes
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        return new StreamsConfig(props);
    }
}
```

---

## Step 4: Build the Kafka Streams Topology

**File:** `src/main/java/com/example/kafka/streams/topology/OrderEnrichmentTopology.java`

```java
package com.example.kafka.streams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.example.kafka.streams.domain.Customer;
import com.example.kafka.streams.domain.EnrichedOrder;
import com.example.kafka.streams.domain.Order;
import com.example.kafka.streams.domain.OrderStats;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Component
public class OrderEnrichmentTopology {

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        
        // Serdes
        JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
        JsonSerde<Customer> customerSerde = new JsonSerde<>(Customer.class);
        JsonSerde<EnrichedOrder> enrichedOrderSerde = new JsonSerde<>(EnrichedOrder.class);
        JsonSerde<OrderStats> statsSerde = new JsonSerde<>(OrderStats.class);
        
        // 1. Build KStream from orders topic
        KStream<String, Order> orders = builder.stream(
            "orders-topic",
            org.apache.kafka.streams.kstream.Consumed.with(
                Serdes.String(),
                orderSerde
            )
        );
        
        // 2. Build KTable from customers (for enrichment)
        KTable<String, Customer> customers = builder.table(
            "customers-topic",
            org.apache.kafka.streams.kstream.Consumed.with(
                Serdes.String(),
                customerSerde
            )
        );
        
        // 3. Join: enrich orders with customer data
        KStream<String, EnrichedOrder> enrichedOrders = orders
            .leftJoin(
                customers,
                (order, customer) -> {
                    if (customer == null) {
                        // Customer not found
                        return new EnrichedOrder(
                            order.getOrderId(),
                            order.getCustomerId(),
                            "UNKNOWN",
                            "UNKNOWN",
                            order.getAmount(),
                            order.getTimestamp()
                        );
                    }
                    return new EnrichedOrder(
                        order.getOrderId(),
                        order.getCustomerId(),
                        customer.getName(),
                        customer.getTier(),
                        order.getAmount(),
                        order.getTimestamp()
                    );
                },
                org.apache.kafka.streams.kstream.Joined.with(
                    Serdes.String(),
                    orderSerde,
                    customerSerde
                )
            );
        
        // 4. Output enriched orders
        enrichedOrders.to(
            "enriched-orders-topic",
            org.apache.kafka.streams.kstream.Produced.with(
                Serdes.String(),
                enrichedOrderSerde
            )
        );
        
        // 5. Aggregate: count orders per customer per minute
        enrichedOrders
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
            .count(Materialized.as("order-counts-store"))
            .toStream()
            .map((windowedKey, count) -> {
                String customerId = windowedKey.key();
                long windowStart = windowedKey.window().startTime().toEpochMilli();
                long windowEnd = windowedKey.window().endTime().toEpochMilli();
                
                OrderStats stats = new OrderStats(
                    customerId,
                    count,
                    windowStart,
                    windowEnd
                );
                return new org.apache.kafka.streams.KeyValue<>(
                    windowedKey.key(),
                    stats
                );
            })
            .to(
                "order-counts-topic",
                org.apache.kafka.streams.kstream.Produced.with(
                    Serdes.String(),
                    statsSerde
                )
            );
        
        // Log topology
        System.out.println("\n=== Kafka Streams Topology ===");
        System.out.println(builder.build().describe());
        System.out.println("===============================\n");
    }
}
```

---

## Step 5: Create Interactive Query Service

**File:** `src/main/java/com/example/kafka/streams/service/InteractiveQueryService.java`

```java
package com.example.kafka.streams.service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class InteractiveQueryService {

    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    public Long getOrderCount(String customerId) {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        
        if (streams == null || !streams.state().isRunning()) {
            throw new IllegalStateException("Kafka Streams is not running");
        }
        
        try {
            ReadOnlyKeyValueStore<String, Long> store = streams.getQueryableStore(
                "order-counts-store",
                QueryableStoreTypes.keyValueStore()
            );
            
            Long count = store.get(customerId);
            return count != null ? count : 0L;
        } catch (Exception e) {
            throw new RuntimeException("Failed to query store: " + e.getMessage(), e);
        }
    }
}
```

---

## Step 6: Create REST Controllers

**File:** `src/main/java/com/example/kafka/streams/controller/OrderProducerController.java`

```java
package com.example.kafka.streams.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.streams.domain.Order;

@RestController
@RequestMapping("/api/streams/orders")
public class OrderProducerController {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    @PostMapping
    public String produceOrder(@RequestBody Order order) {
        kafkaTemplate.send("orders-topic", order.getCustomerId(), order);
        return "Order produced: " + order.getOrderId();
    }
}
```

**File:** `src/main/java/com/example/kafka/streams/controller/CustomerProducerController.java`

```java
package com.example.kafka.streams.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.streams.domain.Customer;

@RestController
@RequestMapping("/api/streams/customers")
public class CustomerProducerController {

    @Autowired
    private KafkaTemplate<String, Customer> kafkaTemplate;

    @PostMapping
    public String produceCustomer(@RequestBody Customer customer) {
        kafkaTemplate.send("customers-topic", customer.getCustomerId(), customer);
        return "Customer produced: " + customer.getCustomerId();
    }
}
```

**File:** `src/main/java/com/example/kafka/streams/controller/InteractiveQueryController.java`

```java
package com.example.kafka.streams.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.streams.service.InteractiveQueryService;

@RestController
@RequestMapping("/api/streams/queries")
public class InteractiveQueryController {

    @Autowired
    private InteractiveQueryService queryService;

    @GetMapping("/order-count/{customerId}")
    public QueryResult getOrderCount(@PathVariable String customerId) {
        Long count = queryService.getOrderCount(customerId);
        return new QueryResult(customerId, count);
    }

    public static class QueryResult {
        public String customerId;
        public Long orderCount;

        public QueryResult(String customerId, Long orderCount) {
            this.customerId = customerId;
            this.orderCount = orderCount;
        }
    }
}
```

---

## Step 7: Configure Application Properties

**File:** `src/main/resources/application.yml`

```yaml
spring:
  application:
    name: kafka-streams-demo
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      bootstrap-servers: localhost:9092
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.example.kafka.streams.domain

server:
  port: 8080

logging:
  level:
    org.apache.kafka: INFO
    org.springframework.kafka: INFO
```

---

## Step 8: Run the Application

Build and start the Spring Boot application:

```bash
# From project root
mvn clean install
mvn spring-boot:run
```

Wait for the startup message:
```
=== Kafka Streams Topology ===
Topology:
  ...
===============================
```

The app is now running and consuming from orders-topic and customers-topic.

---

## Step 9: Test the Streams Application

### Produce a Customer

```bash
curl -X POST http://localhost:8080/api/streams/customers \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "CUST_001",
    "name": "Alice Johnson",
    "email": "alice@example.com",
    "tier": "GOLD"
  }'
```

### Produce Orders

```bash
curl -X POST http://localhost:8080/api/streams/orders \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD_001",
    "customer_id": "CUST_001",
    "amount": 250.50,
    "timestamp": '$(date +%s)'000'
  }'

curl -X POST http://localhost:8080/api/streams/orders \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD_002",
    "customer_id": "CUST_001",
    "amount": 120.75,
    "timestamp": '$(date +%s)'000'
  }'
```

### Query Order Count (Interactive Query)

```bash
curl http://localhost:8080/api/streams/queries/order-count/CUST_001

# Expected response:
# {
#   "customerId": "CUST_001",
#   "orderCount": 2
# }
```

### Verify Output Topics

**Enriched Orders:**

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic enriched-orders-topic \
  --from-beginning \
  --property print.key=true
```

Expected output:
```
CUST_001	{"order_id":"ORD_001","customer_id":"CUST_001","customer_name":"Alice Johnson","customer_tier":"GOLD","amount":250.50,"timestamp":...}
CUST_001	{"order_id":"ORD_002","customer_id":"CUST_001","customer_name":"Alice Johnson","customer_tier":"GOLD","amount":120.75,"timestamp":...}
```

**Order Counts:**

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-counts-topic \
  --from-beginning \
  --property print.key=true
```

Expected output:
```
CUST_001	{"customer_id":"CUST_001","count":1,"window_start":...,"window_end":...}
CUST_001	{"customer_id":"CUST_001","count":2,"window_start":...,"window_end":...}
```

---

## Step 10: Checkpoint

✅ **You've successfully:**
1. Created a Kafka Streams topology with stream-table join
2. Enriched order data with customer information
3. Aggregated orders per customer per time window
4. Queried state stores via an interactive REST endpoint

**Key observations:**
- Join happens as orders arrive
- If a customer exists in the customers topic, orders are enriched; if not, the left join outputs null customer data
- Window-based aggregation counts orders within 1-minute windows
- Interactive queries bypass the output topic (sub-millisecond latency)

---

## Bonus: Advanced Exercises

### Exercise 1: Stream-Stream Join

Modify the topology to join orders with shipments (both KStreams) within a 1-hour window:

```java
KStream<String, Shipment> shipments = builder.stream("shipments-topic");

enrichedOrders.join(
    shipments,
    (order, shipment) -> new OrderShipmentMatched(order, shipment),
    JoinWindows.of(Duration.ofHours(1))
)
.to("order-shipment-matched-topic");
```

### Exercise 2: GlobalKTable for Geo Codes

Create a GlobalKTable of country codes and join orders to add geo information:

```java
GlobalKTable<String, GeoData> geos = builder.globalTable("geo-codes-topic");

enrichedOrders.join(
    geos,
    (order) -> order.getCountryCode(),
    (order, geo) -> new GeoEnrichedOrder(order, geo)
)
.to("geo-enriched-orders");
```

### Exercise 3: Multiple Aggregations

In the same topology, compute:
- Count per customer per minute (done ✓)
- Sum of amounts per customer per minute
- Max order amount per customer per minute

---

## Key Takeaways

!!! success "What You Learned"
    1. **Topology Building** — Define processors, sources, sinks
    2. **Stream-Table Joins** — Enrich streams with stateful data
    3. **Windowing** — Group events by time
    4. **Stateful Aggregations** — Count, sum, reduce with state stores
    5. **Interactive Queries** — Query RocksDB stores without output topics
    6. **Spring Integration** — @EnableKafkaStreams, StreamsBuilder, InteractiveQueryService

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Kafka Streams is not running" | Check application logs; ensure Kafka broker is up |
| Empty output topics | Check topic names in topology vs. property files |
| Interactive query returns 0 | Window may not have closed; wait 1+ minute |
| Schema mismatch errors | Ensure JSON serdes match domain classes |

---

## Next Steps

- Read [09 · Kafka Streams Theory](../theory/09-kafka-streams.md) for deeper concepts
- Explore **Session Windows** (dynamic based on inactivity)
- Implement **Exactly-Once Semantics** (EOS) and measure performance impact
- Add **Prometheus metrics** to monitor your topology


