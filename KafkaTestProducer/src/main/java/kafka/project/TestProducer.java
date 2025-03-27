package kafka.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Properties;

public class TestProducer {
    private static final Logger log = LoggerFactory.getLogger(TestProducer.class.getSimpleName());

    public static void main(String[] args) throws IOException {

        String bootstrapServers = "localhost:9092";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 100; i++) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("topics.test.v1", Integer.toString(i), Integer.toString(i));
            producer.send(producerRecord, (metadata, exception) -> {
                if(exception != null) {
                    log.error(exception.getMessage(), exception);
                }else{
                    log.info("Message sent to topic {}", metadata.topic());
                }
            });

        }

        producer.flush();
        producer.close();
    }

}