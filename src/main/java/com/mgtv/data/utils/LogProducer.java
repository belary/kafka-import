package com.mgtv.data.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class LogProducer  {

  private final String brokers;
  private final String topic;
  private final int maxRecordPerSec;
  private final Throttler throttler;
  private final KafkaProducer<String, String> producer;

  public LogProducer(String brokers, String topic, int maxRecordPerSec) {
    this.brokers = brokers;
    this.topic = topic;
    this.maxRecordPerSec = maxRecordPerSec;

    throttler = new Throttler(maxRecordPerSec);
    producer = new KafkaProducer<>(getProperties());
  }

  public void send(String content) throws InterruptedException {

    ProducerRecord<String, String> record =
          new ProducerRecord<>(topic, content);
    producer.send(record);
    throttler.throttle();
  }

  public void close() {
    producer.close();
  }

  private Properties getProperties() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    return props;
  }
}
