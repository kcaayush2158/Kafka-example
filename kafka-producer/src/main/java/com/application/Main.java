package com.application;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.64:9092");
        pros.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pros.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"com.application.OrderSerializer");
        pros.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,VIPPartitioner.class.getName());
//        pros.setProperty("group.id","OrderGroup");

        pros.setProperty(ProducerConfig.ACKS_CONFIG ,"1");
//        pros.setProperty(ProducerConfig.ACKS_CONFIG ,"all");

        // default memory size 256 mb , data-type bytes ,
        pros.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"2424353");

        pros.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy/gzip/lz4");
        // sends requests 2 times
        pros.setProperty(ProducerConfig.RETRIES_CONFIG, "2");
        //default retry 100 milli
        pros.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"400");
        // default size 16 mb
        pros.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"34532345");
        pros.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"34532345");
        //synchronous way
        KafkaProducer<String, Order> producer = new KafkaProducer<String,Order>(pros);



        Order order = new Order("Aayush","Mac Book Pro",1);


        ProducerRecord<String,Order> record = new ProducerRecord<>("OrderPartitionedTopic",order.getCustomerName() ,order );

        try{
            System.out.println("Sending ...");
            producer.send(record);
            System.out.println(record);
             Order order1 = record.value();
             System.out.println(order1.getCustomerName());
            System.out.println(order1.getQuantity());
            System.out.println(order1.getProduct());
            System.out.println("message sent successfully");

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }

    }

}
