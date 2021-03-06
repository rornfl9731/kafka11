package Jinwoo.kafka.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        //create Procudcer properties
        Properties properties = new Properties();


        //properties.setProperty("bootstrap.servers",bootstrapServer);  아래것이 더 좋고 편해 진짜?
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        //커밋하면 여기가 생기겠지? 오 신기방기 뭐지;ㅇㅇ 푸쉬를 해야는구나

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create 프로듀서
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);


        //record

        ProducerRecord<String,String> record = new ProducerRecord<String, String>("one_topic","hello worldd");

        // 데이터 보내 -> 비동기

        producer.send(record);
        //flush and close
        producer.flush();
        producer.close();
    }
}
