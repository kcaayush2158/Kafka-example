package com.application;

import com.application.callback.OrderCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.application.model.Order;

import java.util.Properties;
import java.util.concurrent.Future;

public class Main {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers","192.168.1.64:9092");
        pros.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        pros.setProperty("value.serializer","com.application.OrderSerializer");

        //synchronous way
        KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(pros);
        Order order = new Order();
        order.setQuantity(10);
        order.setCustomerName("Aayush");
        order.setProduct("MAC BOOK PRO");
//        ProducerRecord<String,Order> record = new ProducerRecord<>("order-topic",order.getCustomerName(),order);

        ProducerRecord<String,Order> record = new ProducerRecord<>("orderCSTopic",order.getCustomerName(),order);
        System.out.println("Sending ...");

        try{
            //synchronous way
           Future<RecordMetadata> recordMetadataFuture =  producer.send(record);
           RecordMetadata recordMetadata = recordMetadataFuture.get();
           System.out.println(recordMetadata.partition());
           System.out.println(recordMetadata.offset());

            //asynchronous way
            producer.send(record,new OrderCallback());
            System.out.println("message sent successfully");

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }

    }

}
