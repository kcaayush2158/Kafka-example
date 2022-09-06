package application;

import application.custome.deserializer.OrderDeserializer;
import application.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {

            Properties pros = new Properties();
            pros.setProperty("bootstrap.servers","192.168.1.64:9092");
            pros.setProperty("key.deserializer", StringDeserializer.class.getName());
            pros.setProperty("value.deserializer", OrderDeserializer.class.getName());
            pros.setProperty("group.id","OrderGroup");
            //synchronous way
            KafkaConsumer<String, Order> consumer = new KafkaConsumer<String, Order>(pros);
            consumer.subscribe(Collections.singleton("orderCSTopic"));

            ConsumerRecords<String,Order> consumerRecords = consumer.poll(Duration.ofSeconds(20));
            for(ConsumerRecord<String,Order> record :consumerRecords){
                String consumerName = record.key();
                Order order= record.value();
                    System.out.println("Product Name : " + order.getProduct());
                    System.out.println("Quantity :"+ order.getQuantity());
            }
            consumer.close();

    }
}
