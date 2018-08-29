import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
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




    public static void main(String[] args) throws InterruptedException {
        new KafkaProducerDemo().produce("topic01");
    }


    //生产数据
    public void produce(String topic) throws InterruptedException {
        final SimpleDateFormat hms = new SimpleDateFormat("hh:mm:ss");
        long flag = 0;
        while (true){
            final String key = String.valueOf(flag);
            final String value = "this is topic01's value : " + hms.format(new Date());

            producer.send(new ProducerRecord<String, String>(topic,key,value));
            System.out.println(value);
            flag ++ ;

            Thread.sleep(2000);
        }

    }
}
