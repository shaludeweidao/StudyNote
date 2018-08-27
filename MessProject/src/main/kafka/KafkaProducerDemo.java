import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerDemo {
    KafkaProducer<String, String> producer = null;

    //初始化kafka参数
    public KafkaProducerDemo(){
        final Properties prop = new Properties();
        prop.put("bootstrap.servers","192.168.111.11:9092,192.168.111.12:9092");
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("","");
        prop.put("","");

        producer = new KafkaProducer<String, String>(prop);
    }




    public static void main(String[] args) {
        new KafkaProducerDemo().produce("topic2");
    }


    //生产数据
    public void produce(String topic){
        for (int i = 10; i<20;i++){
            final String key = String.valueOf(i);
            final String value = "hello kafka : " + key;

            producer.send(new ProducerRecord<String, String>(topic,key,value));
            System.out.println(value);
        }
        producer.close();
    }
}
