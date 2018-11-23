package Jinwoo.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
        String bootstrapServer = "127.0.0.1:9092";
        //create Procudcer properties
        Properties properties = new Properties();


        //properties.setProperty("bootstrap.servers",bootstrapServer);  아래것이 더 좋고 편해
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create 프로듀서
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){
            //record

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("one_topic", "김진우"+Integer.toString(i));

            // 데이터 보내 -> 비동기



            //보낸 데이터가 어디로 갔는지 확인 하는
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("receivec new metadata. \n" +
                                "Topic :" + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while procuce", e);

                    }
                }
            });
        }
        //flush and close
        producer.flush();
        producer.close();
    }
}
