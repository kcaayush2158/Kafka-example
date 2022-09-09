package application;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
            pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.64:9092");
             pros.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            pros.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
            pros.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"OrderGroup");


            KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(pros);
            consumer.subscribe(Collections.singleton("OrderPartitionedTopic"));

            try {

                    ConsumerRecords<String,Order> consumerRecords = consumer.poll(Duration.ofSeconds(20));
                    for(ConsumerRecord<String,Order> record :consumerRecords){
                        String consumerName = record.key();
                        System.out.println(record.value());
                        Order order= record.value();
                        System.out.println("Customer Name : " + consumerName);
                        System.out.println("Product Name : " + order.getProduct());
                        System.out.println("Quantity :"+ order.getQuantity());
                        System.out.println("Partition :"+ order.getPartition());

                }
            }finally {
                consumer.close();
            }




    }
}
