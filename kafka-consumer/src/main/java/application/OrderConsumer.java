package application;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {

            Properties pros = new Properties();
            pros.setProperty("bootstrap.servers","192.168.1.64:9092");
            pros.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
            pros.setProperty("value.deserializer",KafkaAvroDeserializer.class.getName());
            pros.setProperty("schema.registry.url","http://localhost:8081");
            pros.setProperty("group.id","OrderGroup");
//            pros.setProperty("specific.avro.reader","true");

            //synchronous way
            KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(pros);
            consumer.subscribe(Collections.singleton("orderAvroGRTopic"));

            ConsumerRecords<String,GenericRecord> consumerRecords = consumer.poll(Duration.ofSeconds(20));
            for(ConsumerRecord<String,GenericRecord> record :consumerRecords){
                String consumerName = record.key();
                GenericRecord order= record.value();
                    System.out.println("Product Name : " + order.get("product"));
                    System.out.println("Quantity :"+ order.get("quantity"));
            }

            consumer.close();

    }
}
