import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    KafkaConsumer<String, String> consumer;

    public KafkaConsumerDemo(){
        final Properties prop = new Properties();
        prop.put("bootstrap.servers","192.168.111.11:9092");
        prop.put("auto.offset.reset","earliest");
        prop.put("group.id","consumer1");
        prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");


        consumer = new KafkaConsumer<String, String>(prop);
    }


    public static void main(String[] args) {
        new KafkaConsumerDemo().consume("topic2");
    }


    public void consume(String topic){
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s",record.offset(), record.key(), record.value());
                System.out.println();
            }
        }
    }


}
