package com.application;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers","192.168.1.64:9092");
        pros.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        pros.setProperty("value.serializer",KafkaAvroSerializer.class.getName());
        pros.setProperty("schema.registry.url","http://localhost:8081");
        //synchronous way
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String,GenericRecord>(pros);
        Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "    \"name\": \"Order\",\n" +
                "    \"type\": \"record\",\n" +
                "    \"namespace\": \"com.application\",\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"name\": \"customerName\",\n" +
                "            \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"product\",\n" +
                "            \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"quantity\",\n" +
                "            \"type\": \"int\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("customerName","Aayush");
        genericRecord.put("product","Mac Book Pro");
        genericRecord.put("quantity",1)
        ;

        ProducerRecord<String,GenericRecord> record = new ProducerRecord<String,GenericRecord>("orderAvroGRTopic",genericRecord.get("customerName").toString() ,genericRecord);

        try{
            System.out.println("Sending ...");
            producer.send(record);
            System.out.println("message sent successfully");

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }

    }

}
