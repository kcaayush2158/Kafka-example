package application;

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
        pros.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("value.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        pros.setProperty("group.id","OrderGroup");

        //synchronous way
        KafkaConsumer<String,Integer> consumer = new KafkaConsumer<String, Integer>(pros);
        consumer.subscribe(Collections.singleton("OrderTopic"));

        ConsumerRecords<String,Integer> orders = consumer.poll(Duration.ofSeconds(20));
        for(ConsumerRecord<String,Integer> order :orders){
                System.out.println("Product Name : " + order.key());
                System.out.println("Quantity :"+order.value());
        }

    }
}
