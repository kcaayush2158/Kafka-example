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
