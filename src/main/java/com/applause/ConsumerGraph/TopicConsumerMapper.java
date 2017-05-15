package com.applause.ConsumerGraph;

import kafka.coordinator.BaseKey;
import kafka.coordinator.GroupMetadataManager;
import kafka.coordinator.GroupTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;


/**
 * The Kafka consumer that maps topics to consumers by listening on the internal
 * __consumer_offsets topic.
 * This does not support offsets stored in Zookeeper.
 */
public class TopicConsumerMapper implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(TopicConsumerMapper.class);

	public static final String GROUPID = "topic-consumer-mapper";
	public static final String TOPIC = "__consumer_offsets";

	private static final int DEFAULT_TIMEOUT = 5000;

	private KafkaConsumer<String, String> consumer;
	private List<String> topics;
	private String topicFilter = null;
	private String consumerFilter = null;
	private long timeout = 5000;
	private long dataLastUpdated;

	private HashMap<String, ArrayList<String>> topicConsumerGroupMap = new HashMap<String, ArrayList<String>>();

	public HashMap<String, ArrayList<String>> getTopicConsumerGroupMap() {
		return this.topicConsumerGroupMap;
	}

	/**
	 * Create the Kafka consumer based on the passed-in parameters.
	 *
	 * @param bootstrapServers the Kafka servers
	 * @param topicFilter the regex filter to apply to topics
	 * @param consumerFilter the regex filter to apply to consumers
	 * @param timeout the consuemr.poll() timeout
	 */
	public TopicConsumerMapper(String bootstrapServers, String topicFilter, String consumerFilter, long timeout) {
		this.topicFilter = topicFilter == null ? "" : topicFilter;
		this.consumerFilter = consumerFilter == null ? "" : consumerFilter;
		this.timeout = timeout < 1 ? this.DEFAULT_TIMEOUT : timeout;

		LOGGER.debug("Bootstrap servers: " + bootstrapServers);
		LOGGER.debug("Topic filter: " + this.topicFilter);
		LOGGER.debug("Consumer filter: " + this.consumerFilter);
		LOGGER.debug("Consumer.poll() timeout: " + this.timeout);

		this.topics = Arrays.asList(this.TOPIC);
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", this.GROUPID);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());

		LOGGER.info("Creating Consumer with properties: " + props.toString());
		this.consumer = new KafkaConsumer(props);
	}

	public void run() {
		try {
			/**
			 * Subscribe to the internal __consumer_offsets topic.
			 */
			consumer.subscribe(topics, new ConsumerRebalanceListener() {
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					// NoOp
				}

				/**
				 * Always go to the beginning of the partitions to ensure we account for all
				 * consumers.
				 */
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					consumer.seekToBeginning(partitions);
				}
			});

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(timeout);
				for (ConsumerRecord<String, String> record : records) {
					this.dataLastUpdated = System.currentTimeMillis();

					// TODO: add message timestamps to output. this requires ui work.
					long recordTimestamp = record.timestamp();
					TimestampType recordTimestampType = record.timestampType();

					BaseKey baseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key().getBytes()));
					if (baseKey.key() instanceof GroupTopicPartition) {
						GroupTopicPartition gtp = (GroupTopicPartition) baseKey.key();
						String topic = gtp.topicPartition().topic();
						String group = gtp.group();

						if (!topic.matches(this.topicFilter)) {
							if (!topicConsumerGroupMap.containsKey(topic)) {
								topicConsumerGroupMap.put(topic, new ArrayList<String>(Arrays.asList(group)));
							}

							ArrayList<String> groupList = topicConsumerGroupMap.get(topic);
							if (!group.matches(this.consumerFilter) && !groupList.contains(group)) {
								groupList.add(group);
								topicConsumerGroupMap.put(topic, groupList);
							}
						}
					}
				}
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}
	}

	/**
	 * Ensure we exit the consumer correctly.
	 */
	public void shutdown() {
		consumer.wakeup();
	}

	/**
	 * Fetch the last time we updated our data.
	 *
	 * @return the epoch millis representing the last time the consumer fetched new data from Kafka.
	 */
	public long getDataLastUpdated() {
		return this.dataLastUpdated;
	}
}