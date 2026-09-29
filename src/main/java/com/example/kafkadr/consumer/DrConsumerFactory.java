package com.example.kafkadr.consumer;

import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.config.KafkaDrConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.function.Function;

/**
 * Creates {@link DrKafkaConsumer} instances for code that manages its own
 * {@code subscribe → poll → commit} loop but needs DR cluster switching.
 *
 * <pre>
 * try (DrKafkaConsumer&lt;String, String&gt; consumer = factory.create(props)) {
 *     consumer.subscribe(List.of(topic));
 *     while (running) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofSeconds(1));
 *         ...
 *     }
 * }
 * </pre>
 */
public class DrConsumerFactory {

    private final ClusterManager clusterManager;
    private final KafkaDrConfig config;
    private final GroupInstanceIds instanceIds;
    private final Function<Properties, Consumer<?, ?>> consumerCreator;

    public DrConsumerFactory(ClusterManager clusterManager, KafkaDrConfig config) {
        this(clusterManager, config, new GroupInstanceIds(config));
    }

    /**
     * @param instanceIds shared with {@link DrConsumerManager} so that no two consumers in the
     *                    process get the same {@code group.instance.id}
     */
    public DrConsumerFactory(ClusterManager clusterManager, KafkaDrConfig config, GroupInstanceIds instanceIds) {
        this(clusterManager, config, instanceIds, props -> new KafkaConsumer<>(props));
    }

    /**
     * @param consumerCreator creates the underlying consumer from resolved properties;
     *                        tests can supply a {@code MockConsumer} here
     */
    public DrConsumerFactory(ClusterManager clusterManager, KafkaDrConfig config, GroupInstanceIds instanceIds,
                             Function<Properties, Consumer<?, ?>> consumerCreator) {
        this.clusterManager = clusterManager;
        this.config = config;
        this.instanceIds = instanceIds;
        this.consumerCreator = consumerCreator;
    }

    /**
     * @param props consumer properties (group.id, deserializer class names, etc.).
     *              {@code bootstrap.servers} is ignored — it comes from the active cluster.
     */
    @SuppressWarnings("unchecked")
    public <K, V> DrKafkaConsumer<K, V> create(Properties props) {
        Function<Properties, Consumer<K, V>> creator = p -> (Consumer<K, V>) consumerCreator.apply(p);
        return new DrKafkaConsumer<>(clusterManager, config, props, instanceIds, creator);
    }
}
