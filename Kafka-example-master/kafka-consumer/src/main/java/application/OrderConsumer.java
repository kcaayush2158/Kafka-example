package application;

import application.custome.deserializer.OrderDeserializer;
import application.model.Order;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class OrderConsumer {
    public static void main(String[] args) {

            Properties pros = new Properties();
            pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.64:9092");
            pros.setProperty("key.deserializer", StringDeserializer.class.getName());
            pros.setProperty("value.deserializer", OrderDeserializer.class.getName());
            pros.setProperty("group.id","OrderGroup");
//        pros.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"order-producer-1");
        pros.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"10241283");
        pros.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"200");
        pros.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"1000");
        pros.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"3000");

        pros.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"1MB");
        pros.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")  ;
        pros.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,"OrderConsumer");
        pros.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        pros.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,RoundRobinAssignor.class.getName());

            //synchronous way

          Map<TopicPartition , OffsetAndMetadata> currentOffsets = new HashMap<>();


            KafkaConsumer<String, Order> consumer = new KafkaConsumer<String, Order>(pros);

            class RebalancedHandler implements ConsumerRebalanceListener{

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    consumer.commitSync(currentOffsets);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

                }
            }


            consumer.subscribe(Collections.singleton("OrderTopic"),new RebalancedHandler());

            ConsumerRecords<String,Order> consumerRecords = consumer.poll(Duration.ofSeconds(20));
            int count = 0;
            for(ConsumerRecord<String,Order> record :consumerRecords){
                String consumerName = record.key();
                Order order= record.value();
                    System.out.println("Product Name : " + order.getProduct());
                    System.out.println("Quantity :"+ order.getQuantity());


                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()) ,
                            new OffsetAndMetadata(record.offset() + 1));


                if(count % 10 == 0) {
                     consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {

                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if (exception != null) {
                                System.out.println("Commit failed for offset" + offsets);
                            }

                        }
                });


                }
                count++;
            }


            consumer.close();

    }
}
