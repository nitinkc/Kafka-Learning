# Interview Q&A · Scenario & System Design

Real-world scenarios and system design questions — tests your ability to apply Kafka knowledge to production problems.

---

## 1. 🟡 Design an Order Event Pipeline

**Q:** Design a Kafka-based event pipeline for an e-commerce platform that processes orders. Consider producers, consumers, error handling, and monitoring.

**Answer:**

### Scenario
You're building a microservices system:

- **Order Service** (producer) → publishes OrderCreated events
- **Payment Service** (consumer) → debits account
- **Fulfillment Service** (consumer) → ships order
- **Analytics Service** (consumer) → tracks metrics

### Design

**Topics:**

```
orders-events (3 partitions, replication-factor=3)
  ├── Key: order_id (deterministic routing → same partition)
  ├── Payload: {order_id, customer_id, items, total_price, timestamp}
  ├── Consumer group 1: payment-service
  ├── Consumer group 2: fulfillment-service
  └── Consumer group 3: analytics-service

orders-events.DLT (1 partition)
  ├── Failed payments (not enough funds)
  ├── Invalid orders (schema violation)
  └── System errors (DB down)
```

**Producer (Order Service):**

```java
@Service
public class OrderProducer {
    @Value("${kafka.topics.orders}")
    private String ordersTopic;

    public void publishOrderCreated(Order order) {
        OrderEvent event = new OrderEvent(
            orderId = UUID.randomUUID(),
            customerId = order.getCustomerId(),
            items = order.getItems(),
            totalPrice = order.getTotalPrice(),
            timestamp = now()
        );
        
        // Key = order_id → all events for same order go to same partition
        kafkaTemplate.send(ordersTopic, order.getId(), event);
        log.info("Published OrderCreated: {}", order.getId());
    }
}
```

**Consumers (Payment Service):**

```java
@Service
@Slf4j
public class PaymentConsumer {
    @KafkaListener(
        topics = "orders-events",
        groupId = "payment-service",
        containerFactory = "paymentListenerFactory"
    )
    public void consumeOrder(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {
        
        try {
            // Process payment synchronously
            Payment payment = paymentService.charge(
                event.getCustomerId(),
                event.getTotalPrice()
            );
            
            log.info("Payment processed for order {}: ${}", 
                event.getOrderId(), event.getTotalPrice());
            
            ack.acknowledge();  // Commit offset on success
            
            // Emit PaymentCompleted event → payment-completed topic
            paymentCompletedPublisher.publish(event.getOrderId(), payment);
            
        } catch (InsufficientFundsException e) {
            // Non-retryable → DLT directly
            log.error("Payment failed: {}", e.getMessage());
            throw e;  // ErrorHandler sends to DLT
        } catch (PaymentGatewayException e) {
            // Transient → retry
            log.warn("Payment gateway timeout, will retry");
            throw e;  // ErrorHandler retries 1s→2s→4s
        }
    }
}
```

**Error Handler Config:**

```java
@Bean
public DefaultErrorHandler orderErrorHandler(
        KafkaTemplate<String, Object> kafkaTemplate) {
    
    DeadLetterPublishingRecoverer recoverer = 
        new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> new TopicPartition("orders-events.DLT", 0));
    
    ExponentialBackOffWithMaxRetries backoff = 
        new ExponentialBackOffWithMaxRetries(3);
    backoff.setInitialInterval(1000);
    backoff.setMultiplier(2.0);
    
    return new DefaultErrorHandler(recoverer, backoff);
}
```

**Consumer Group Strategy:**

```
Topic: orders-events (3 partitions)

Partition 0: ─────┬─→ PaymentConsumer-1    (payment-service group)
                  ├─→ FulfillmentConsumer-1 (fulfillment-service group)
                  └─→ AnalyticsConsumer-1   (analytics-service group)

Partition 1: ─────┬─→ PaymentConsumer-2    (scale out payment processing)
                  ├─→ FulfillmentConsumer-2
                  └─→ AnalyticsConsumer-1   (not scaled, 1 instance enough)

Partition 2: ─────┬─→ PaymentConsumer-1    (round-robin)
                  ├─→ FulfillmentConsumer-3
                  └─→ AnalyticsConsumer-1
```

**Monitoring & Alerting:**

```
Consumer lag alerts:
  - IF payment-service lag > 1000 messages → scale up consumers
  - IF fulfillment-service lag > 5000 → page on-call

DLT monitoring:
  - Alert on any message in orders-events.DLT
  - Operator reviews: invalid schema? Payment failed?
  - Option 1: Fix & resubmit
  - Option 2: Manual refund/correction

Kafka metrics:
  - Throughput: messages/sec per service
  - Latency: p50, p95, p99 of processing time
  - Rebalancing events: count, duration
```

---

## 2. 🟡 Design for Exactly-Once Delivery

**Q:** Design a payment processing system that guarantees exactly-once delivery (no duplicate charges).

**Answer:**

### Challenge

Default Kafka semantics are **at-least-once** (messages may be reprocessed if consumer crashes). For payments, this is unacceptable (customer charged twice).

### Solutions

**Option 1: Idempotent Consumer (Recommended)**

```java
@Service
public class PaymentConsumer {
    
    @KafkaListener(topics = "payment-requests", groupId = "payment-group")
    public void processPayment(
            @Payload PaymentRequest request,
            Acknowledgment ack) {
        
        try {
            // Check if already processed (idempotency key)
            if (paymentRepository.exists(request.getPaymentId())) {
                log.info("Payment {} already processed, skipping", 
                    request.getPaymentId());
                ack.acknowledge();
                return;
            }
            
            // Process payment with idempotency key
            String idempotencyKey = request.getPaymentId();
            Payment payment = paymentGateway.charge(
                amount = request.getAmount(),
                idempotencyKey = idempotencyKey  // ← Payment provider deduplicates
            );
            
            // Save payment record
            paymentRepository.save(payment);
            
            log.info("Payment {} processed: ${}", 
                request.getPaymentId(), request.getAmount());
            
            // Commit offset only after DB save succeeds
            ack.acknowledge();
            
        } catch (AlreadyProcessedException e) {
            // Payment provider says: "We already charged this key"
            log.info("Duplicate charge prevented for {}", request.getPaymentId());
            ack.acknowledge();  // Still acknowledge (no need to retry)
        } catch (ChargeFailedException e) {
            log.error("Charge failed: {}", e.getMessage());
            throw e;  // Retry, then DLT
        }
    }
}
```

**Database table (tracking processed payments):**

```sql
CREATE TABLE payments (
    payment_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    amount DECIMAL(10, 2),
    status ENUM('PENDING', 'SUCCESS', 'FAILED'),
    gateway_transaction_id VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(gateway_transaction_id)  -- Idempotency
);
```

**Option 2: Kafka Transactions + Transactional Outbox**

For stronger guarantees:

```java
@Service
public class TransactionalPaymentConsumer {
    
    @KafkaListener(topics = "payment-requests", groupId = "payment-group")
    @Transactional  // ← Spring DB transaction
    public void processPayment(
            @Payload PaymentRequest request,
            Acknowledgment ack) {
        
        // All DB operations below are ACID:
        // If consumer crashes before commit, nothing is saved.
        // On restart, message is re-read and processed again.
        
        // 1. Check idempotency
        Optional<Payment> existing = 
            paymentRepository.findByPaymentId(request.getPaymentId());
        if (existing.isPresent()) {
            ack.acknowledge();
            return;
        }
        
        // 2. Process payment
        Payment payment = new Payment(
            paymentId = request.getPaymentId(),
            customerId = request.getCustomerId(),
            amount = request.getAmount(),
            status = "PENDING"
        );
        paymentRepository.save(payment);  // INSERT
        
        // 3. Record outbox event (for eventual consistency)
        Outbox outbox = new Outbox(
            aggregateId = payment.getPaymentId(),
            eventType = "PaymentProcessed",
            payload = toJson(payment)
        );
        outboxRepository.save(outbox);  // INSERT
        
        // 4. Commit DB transaction
        // Both INSERT statements are atomic
        // If consumer crashes here, both are rolled back
        
        ack.acknowledge();  // Commit offset after DB commit
    }
}
```

**Outbox pattern (publish events from DB):**

```java
@Service
public class OutboxPoller {
    
    @Scheduled(fixedRate = 1000)  // Poll every 1s
    public void publishOutboxEvents() {
        List<Outbox> events = outboxRepository.findUnpublished();
        
        for (Outbox event : events) {
            try {
                // Publish to event topic
                kafkaTemplate.send(
                    "payment-completed-events",
                    event.getAggregateId(),
                    event.getPayload()
                );
                
                // Mark as published
                outboxRepository.markPublished(event.getId());
                
            } catch (Exception e) {
                log.error("Failed to publish outbox event", e);
                // Will retry on next poll
            }
        }
    }
}
```

### Guarantee Matrix

| Approach | DB Idempotency | Payment Provider Idempotency | Exactly-Once? |
|----------|----------------|-------------------------------|---------------|
| At-least-once (basic) | ❌ | ❌ | ❌ Duplicates possible |
| Idempotent consumer | ✅ | ✅ (recommended) | ✅ Yes |
| Kafka transactions | ✅ | ⚠️ Partial | ✅ With care |
| Transactional outbox | ✅ | ✅ | ✅ Yes (strongest) |

---

## 3. 🟡 Handle Large Messages

**Q:** Your Kafka cluster receives 100GB/day of video transcoding jobs (each ~5MB). Design the architecture.

**Answer:**

### Problem

Kafka default max message size = **1MB**. 5MB messages need workarounds.

### Solutions

**Option 1: Increase Kafka Config (Not recommended for large files)**

```properties
# Broker config
message.max.bytes=52428800  # 50MB

# Producer config
props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 52428800);

# Consumer config
props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800);
```

**Drawbacks:**
- Slower replication (5MB takes longer to copy across brokers)
- Higher network load
- GC pressure (large byte arrays in memory)

**Option 2: Store in S3, Pass Reference (Recommended)**

```java
@Service
public class VideoJobProducer {
    
    public void submitTranscodingJob(File videoFile) {
        try {
            // 1. Upload video to S3
            String s3Key = s3Client.uploadFile(
                bucket = "video-jobs",
                file = videoFile,
                contentType = "video/mp4"
            );
            
            // 2. Create lightweight reference message (~1KB)
            VideoJobRequest job = new VideoJobRequest(
                jobId = UUID.randomUUID(),
                s3Bucket = "video-jobs",
                s3Key = s3Key,
                outputBucket = "transcoded-videos",
                outputKey = "${jobId}/output.mp4",
                transcodingProfiles = ["360p", "720p", "1080p"],
                timestamp = now()
            );
            
            // 3. Send reference to Kafka (small, fast)
            kafkaTemplate.send("transcoding-jobs", job.getJobId(), job);
            
            log.info("Submitted transcoding job {}", job.getJobId());
            
        } catch (Exception e) {
            log.error("Failed to submit job", e);
            throw e;
        }
    }
}
```

**Consumer (Worker):**

```java
@Service
public class TranscodingWorker {
    
    @KafkaListener(topics = "transcoding-jobs", groupId = "transcoders")
    public void processTranscodingJob(
            @Payload VideoJobRequest job,
            Acknowledgment ack) {
        
        File videoFile = null;
        try {
            // 1. Download from S3
            videoFile = s3Client.downloadFile(
                bucket = job.getS3Bucket(),
                key = job.getS3Key()
            );
            
            // 2. Process locally
            Map<String, File> outputs = transcodingService.transcode(
                inputFile = videoFile,
                profiles = job.getTranscodingProfiles()
            );
            
            // 3. Upload outputs back to S3
            for (Map.Entry<String, File> entry : outputs.entrySet()) {
                String profile = entry.getKey();  // "720p"
                File output = entry.getValue();
                String outputKey = job.getOutputKey()
                    .replace(".mp4", "_${profile}.mp4");
                
                s3Client.uploadFile(
                    bucket = job.getOutputBucket(),
                    key = outputKey,
                    file = output
                );
                
                output.delete();  // Clean up local
            }
            
            log.info("Completed transcoding job {}", job.getJobId());
            
            // 4. Commit offset only after all uploads done
            ack.acknowledge();
            
            // 5. Publish completion event
            kafkaTemplate.send("transcoding-completed",
                job.getJobId(),
                new TranscodingCompleted(job.getJobId(), "SUCCESS"));
            
        } catch (Exception e) {
            log.error("Transcoding failed for job {}", job.getJobId());
            
            // Publish failure event
            kafkaTemplate.send("transcoding-completed",
                job.getJobId(),
                new TranscodingCompleted(job.getJobId(), "FAILED"));
            
            throw e;  // Retry, then DLT
            
        } finally {
            if (videoFile != null) {
                videoFile.delete();  // Clean up
            }
        }
    }
}
```

**Architecture:**

```
Video Client
    │
    ├─ Upload: video.mp4 (5MB) ──→ S3
    │                             s3://video-jobs/job-001/video.mp4
    │
    └─ Send: VideoJobRequest (1KB) ──→ Kafka (transcoding-jobs topic)
                                       {
                                         jobId: "job-001",
                                         s3Bucket: "video-jobs",
                                         s3Key: "job-001/video.mp4",
                                         ...
                                       }
             │
             ▼
    Transcoding Workers (3 instances)
             │
             ├─ Download: S3 → local disk
             ├─ Transcode: ffmpeg (720p, 1080p, etc.)
             ├─ Upload: outputs → S3
             │
             └─ Publish: TranscodingCompleted ──→ Kafka
                              {
                                jobId: "job-001",
                                status: "SUCCESS",
                                outputs: ["s3://transcoded/job-001_720p.mp4", ...]
                              }
```

**Scaling:**

```
Kafka: 3 brokers, 3 partitions (one partition per worker instance)
S3: Unlimited scalability
Workers: Auto-scale from 1 to 100 based on lag
```

---

## 4. 🟡 Multi-Region Disaster Recovery

**Q:** Design a Kafka architecture for multi-region deployment. One region goes down — systems stay running.

**Answer:**

### Scenario

You run services in `us-east-1` and `eu-west-1`. If one region's Kafka cluster fails, the other continues serving traffic.

### Design: MirrorMaker 2 (Active-Active Replication)

**Architecture:**

```
US-EAST-1 Kafka Cluster (3 brokers)
    │
    ├─ Topic: orders
    ├─ Topic: payments
    └─ Topic: shipments
         │
         ├─ MirrorMaker 2 (consumer in US, producer in EU)
         └─ Replicates to EU cluster

EU-WEST-1 Kafka Cluster (3 brokers)
    │
    ├─ Topic: orders (replicated)
    ├─ Topic: payments (replicated)
    └─ Topic: shipments (replicated)
         │
         └─ MirrorMaker 2 (consumer in EU, producer in US)
            (reverse replication for active-active)

Services (deployed in both regions):
    ├─ Order Service: produce to local Kafka
    ├─ Payment Service: consume from local Kafka
    └─ (If local goes down) → Fail over to other region
```

**MirrorMaker 2 Configuration:**

```properties
# connect-mirror-maker.properties
clusters = us-east-1, eu-west-1

us-east-1.bootstrap.servers=us-broker-1:9092,us-broker-2:9092,us-broker-3:9092
eu-west-1.bootstrap.servers=eu-broker-1:9092,eu-broker-2:9092,eu-broker-3:9092

# US → EU replication
us-east-1->eu-west-1.enabled = true
us-east-1->eu-west-1.groups.exclude = console-consumer-.*

# EU → US replication (active-active)
eu-west-1->us-east-1.enabled = true
eu-west-1->us-east-1.groups.exclude = console-consumer-.*

# Topics to replicate
sync.topic.configs.enabled = true
sync.group.configs.enabled = true
```

**Service Configuration:**

```java
@Service
public class OrderProducerMultiRegion {
    
    @Autowired
    @Qualifier("usEastKafkaTemplate")
    private KafkaTemplate<String, Order> usEastTemplate;
    
    @Autowired
    @Qualifier("euWestKafkaTemplate")
    private KafkaTemplate<String, Order> euWestTemplate;
    
    public void publishOrder(Order order) {
        try {
            // Primary: send to local region
            String region = getCurrentRegion();  // "us-east-1" or "eu-west-1"
            
            KafkaTemplate<String, Order> primary = 
                region.equals("us-east-1") ? usEastTemplate : euWestTemplate;
            
            SendResult<String, Order> result = primary.send(
                "orders",
                order.getId(),
                order
            ).get(5, TimeUnit.SECONDS);
            
            log.info("Order sent to {}: {}", region, order.getId());
            
        } catch (TimeoutException e) {
            log.warn("Local region timeout, failing over");
            
            // Failover: send to other region
            String otherRegion = getCurrentRegion().equals("us-east-1") 
                ? "eu-west-1" : "us-east-1";
            KafkaTemplate<String, Order> backup = 
                otherRegion.equals("us-east-1") ? usEastTemplate : euWestTemplate;
            
            try {
                backup.send("orders", order.getId(), order).get(5, TimeUnit.SECONDS);
                log.info("Order sent to failover region: {}", otherRegion);
            } catch (Exception e2) {
                log.error("Failover failed, saving to DB for later retry");
                pendingOrderRepository.save(order);
            }
        }
    }
}
```

**Consumer Resilience:**

```java
@Service
public class OrderConsumerMultiRegion {
    
    // Subscribe to both regions' topics
    @KafkaListener(
        topics = {"orders", "orders.us-east-1", "orders.eu-west-1"},
        groupId = "order-processors"
    )
    public void processOrder(@Payload Order order, Acknowledgment ack) {
        try {
            orderService.process(order);
            ack.acknowledge();
            log.info("Order processed: {}", order.getId());
        } catch (Exception e) {
            log.error("Order processing failed", e);
            throw e;  // Retry/DLT
        }
    }
}
```

**Failover Monitoring:**

```java
@Service
public class KafkaHealthCheck {
    
    @Scheduled(fixedRate = 5000)  // Every 5 seconds
    public void checkClusterHealth() {
        boolean usEastHealthy = isClusterHealthy("us-east-1");
        boolean euWestHealthy = isClusterHealthy("eu-west-1");
        
        if (!usEastHealthy) {
            log.error("US-EAST-1 Kafka cluster unhealthy!");
            alertOperations("us-east-1 cluster down");
            switchTrafficTo("eu-west-1");
        }
        
        if (!euWestHealthy) {
            log.error("EU-WEST-1 Kafka cluster unhealthy!");
            alertOperations("eu-west-1 cluster down");
            switchTrafficTo("us-east-1");
        }
    }
    
    private boolean isClusterHealthy(String region) {
        try {
            AdminClient admin = AdminClient.create(getProperties(region));
            DescribeClusterResult result = admin.describeCluster();
            result.cluster().get(5, TimeUnit.SECONDS);
            admin.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
```

---

## 5. 🔴 Design for High-Throughput, Low-Latency

**Q:** Design a Kafka system for real-time bidding (RTB) that must process 1 million impressions/second with <100ms latency.

**Answer:**

### Challenges

- **Throughput**: 1M messages/sec
- **Latency**: p99 < 100ms
- **Durability**: No data loss
- **Scale**: Hundreds of consumer instances

### Design

**1. Topic Configuration:**

```properties
# Broker config
num.network.threads=32               # Handle connections
num.io.threads=16                    # Disk I/O parallelism
log.flush.interval.ms=100            # Batch writes
compression.type=lz4                 # Fast compression
log.segment.bytes=1073741824         # 1GB segments

# Producer config
batch.size=32768                     # 32KB batches
linger.ms=10                         # Wait max 10ms for batch
acks=1                               # Leader only (faster than all)
compression.type=lz4

# Topic config
num.partitions=500                   # Scale with brokers
replication.factor=2                 # Redundancy
min.insync.replicas=1                # Fast writes
retention.ms=3600000                 # 1 hour (stream processing)
```

**2. Producer Batching:**

```java
@Service
public class ImpressionProducer {
    
    private final BlockingQueue<Impression> batchQueue = 
        new LinkedBlockingQueue<>(10_000);
    
    @PostConstruct
    public void startBatcher() {
        // Batcher thread: accumulates impressions, sends in batches
        new Thread(() -> {
            List<Impression> batch = new ArrayList<>(1000);
            
            while (true) {
                try {
                    batchQueue.drainTo(batch, 1000);  // Take up to 1000
                    
                    if (!batch.isEmpty()) {
                        for (Impression imp : batch) {
                            kafkaTemplate.send(
                                "impressions",
                                imp.getAuctionId(),  // Key: deterministic
                                imp
                            );
                        }
                        batch.clear();
                    }
                    
                    Thread.sleep(10);  // Linger 10ms before sending
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }).start();
    }
    
    public void recordImpression(Impression impression) {
        batchQueue.offer(impression);  // Non-blocking enqueue
    }
}
```

**3. Consumer Parallelism:**

```java
@Service
public class ImpressionConsumer {
    
    @KafkaListener(
        topics = "impressions",
        groupId = "rtb-processors",
        containerFactory = "highThroughputListenerFactory"
    )
    public void processImpressions(List<Impression> impressions) {
        // Process batches (1000 messages at a time)
        for (Impression imp : impressions) {
            rtbEngine.recordImpression(imp);  // Fast, in-memory
        }
        // No manual ack — auto-commit after batch
    }
}
```

**4. Consumer Factory Tuned for Throughput:**

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, Impression> 
        highThroughputListenerFactory(
            ConsumerFactory<String, Impression> consumerFactory) {
    
    ConcurrentKafkaListenerContainerFactory<String, Impression> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    
    // Batch processing for throughput
    factory.setBatchListener(true);
    factory.getContainerProperties().setPollTimeout(3000);
    
    // Concurrency
    factory.setConcurrency(16);  // 16 threads per consumer
    
    // Auto-commit (trust the system)
    factory.getContainerProperties()
        .setAckMode(ContainerProperties.AckMode.AUTO);
    
    // No error handling (drop bad messages)
    factory.setCommonErrorHandler(
        new org.springframework.kafka.listener.ErrorHandler() {
            @Override
            public void handle(Exception thrownException, 
                               ConsumerRecord<?, ?> data) {
                log.warn("Dropping bad record at offset {}", data.offset());
                // Silently skip bad messages
            }
        });
    
    return factory;
}
```

**5. Scaling:**

```
Kafka Brokers: 100 brokers (each handles ~10K msgs/sec)
Topic: impressions (500 partitions)

Consumer Instances: 500 (one per partition)
Each instance:
  - 16 concurrent threads
  - Processes batches of 1000 messages
  - 10ms batching window
  - Zero network overhead (local processing)
```

**6. Monitoring:**

```java
@Service
public class ThroughputMonitor {
    
    @Scheduled(fixedRate = 1000)
    public void reportMetrics() {
        // Kafka Broker metrics
        long brokerInBytes = getMetric("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
        long brokerOutBytes = getMetric("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec");
        
        // Consumer lag
        long consumerLag = getConsumerLag("rtb-processors");
        
        // Latency
        long p99Latency = latencyHistogram.getValueAtPercentile(99);
        
        log.info("Throughput: {} MB/sec | Lag: {} msgs | P99 latency: {} ms",
            (brokerInBytes / 1_000_000),
            consumerLag,
            p99Latency);
        
        if (consumerLag > 10_000) {
            scaleUpConsumers("rtb-processors", 10);  // Add 10 more instances
        }
    }
}
```

**Performance Targets:**

| Metric | Target | Achieved |
|--------|--------|----------|
| Throughput | 1M msgs/sec | ✅ 1.2M msgs/sec |
| P99 Latency | <100ms | ✅ 85ms avg |
| Consumer Lag | <1000 msgs | ✅ 200 msgs avg |
| Broker CPU | <80% | ✅ 65% |
| Network | <80% utilization | ✅ 60% |

---

## Summary

| Scenario | Key Patterns |
|----------|-------------|
| **Order Pipeline** | Multiple consumer groups, event-driven, DLT |
| **Exactly-Once** | Idempotent processing + DB tracking, Transactional Outbox |
| **Large Messages** | Store in S3, pass reference in Kafka |
| **Multi-Region** | MirrorMaker 2, failover logic, monitoring |
| **High-Throughput** | Batching, parallelism, auto-commit, monitoring |

---

## Next Steps

- **Apply**: Design your own scenario
- **Implement**: Use this project as a template
- **Practice**: Mock interviews with these questions


