package com.applause.ConsumerGraph;


import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.*;

public class ConsumerGraphTest {

	@Test
	public void testIsKafkaAlive() {
		String bootstrapServers = "unknownKafkaServer13:9092";
		assertFalse(ConsumerGraph.isKafkaAlive(bootstrapServers));
	}

	@Test
	public void testLoadProperties() {
		try {
			Properties properties = ConsumerGraph.loadProperties("config.properties");
			assertTrue(true);
		} catch (Exception ioe) {
			fail();
		}

		try {
			Properties properties = ConsumerGraph.loadProperties("foo.properties");
			fail();
		} catch (Exception ioe) {
			assertTrue(true);
		}
	}

	@Test
	public void testCheckProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "kafka1");
		properties.put("port", "9092");

		ArrayList<String> missingProperties = ConsumerGraph.checkProperties(properties);
		assertTrue(missingProperties.size() == 0);

		properties.remove("bootstrap.servers");
		missingProperties = ConsumerGraph.checkProperties(properties);
		assertTrue(missingProperties.size() == 1);
		assertTrue(missingProperties.contains("bootstrap.servers"));

		properties.remove("port");
		missingProperties = ConsumerGraph.checkProperties(properties);
		assertTrue(missingProperties.size() == 2);
		assertTrue(missingProperties.contains("bootstrap.servers") && missingProperties.contains("port"));
	}

	@Test
	public void testGetConfig() {
		String[] args = new String[]{"--config", "config.properties"};
		String configFile = ConsumerGraph.getConfig(args);
		assertTrue(configFile.equalsIgnoreCase("config.properties"));

		args = new String[]{"-c", "config.properties"};
		configFile = ConsumerGraph.getConfig(args);
		assertTrue(configFile.equalsIgnoreCase("config.properties"));
	}
}
