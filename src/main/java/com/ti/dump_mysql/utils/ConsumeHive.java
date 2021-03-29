package com.ti.dump_mysql.utils;

import com.ti.dump_mysql.hive.QueryHive;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.SQLException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumeHive {
    QueryHive queryHive=new QueryHive();
    public  void consume(String url,String topic) throws ParseException, SQLException, ClassNotFoundException {
        Properties props = new Properties();
        props.put("bootstrap.servers", url);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));

        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                queryHive.begin();
            }
        }
        consumer.close();
    }

    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException {
        new ConsumeHive().consume("hbase:9092","platform_hive");
    }
}
