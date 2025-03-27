package kafka.project;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TestConsumer {
    private static final Logger log = LoggerFactory.getLogger(TestProducer.class.getSimpleName());

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String groupId = "testGroup";
        String topic = "topics.test.v1";
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down");
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                log.info("Polling for data.");
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String,String> record : records){
                    log.info(record.key() + " " + record.value());
                }
            }
        }catch (WakeupException e){
            log.info("Shutdown signal");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        }finally {
            consumer.close();
            log.info("Consumer closed");
        }

    }

}
