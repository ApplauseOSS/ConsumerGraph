package com.applause.ConsumerGraph;


import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Properties;


/**
 * A simple program to graph Kafka topics to their consumers.
 */
public class ConsumerGraph {
	private static Logger LOGGER = LoggerFactory.getLogger(ConsumerGraph.class);

	private static final int SERVER_TIMEOUT = 30;

	public static final String DEFAULT_CLUSTER_NAME = "Kafka";
	public static final String DEFAULT_UI_STYLE = "graph";

	public static final String FILTERS_TOPIC = "filters.topic";
	public static final String FILTERS_CONSUMER = "filters.consumer";
	public static final String CLUSTER_NAME = "cluster.name";
	public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	public static final String TIMEOUT = "timeout";
	public static final String PORT = "port";
	public static final String UI_STYLE = "ui.style";

	public ConsumerGraph() { }

	/**
	 * Test if the Kafka servers are available.
	 * Note that this fails if any server is unavailable.
	 *
	 * @param bootstrapServers the list of Kafka servers to test, comma separated
	 * @return true if all servers are reachable, false otherwise
	 */
	protected static boolean isKafkaAlive(String bootstrapServers) {
		for (String server : bootstrapServers.split(",")) {
			try {
				String[] hostPort = server.split(":");
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])), SERVER_TIMEOUT);
			} catch (IOException e) {
				return false; // Either timeout or unreachable or failed DNS lookup.
			}
		}

		return true;
	}

	/**
	 * Load properties from the specified configuration file.
	 *
	 * @param configFile the configuration file to load, including path
	 * @return a Properties object loaded from the configuration file
	 * @throws IOException
	 */
	protected static Properties loadProperties(String configFile) throws IOException {
		InputStream input = null;
		Properties properties = null;

		try {
			input = new FileInputStream(configFile);
			properties = new Properties();
			properties.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		if (properties == null) {
			throw new RuntimeException("Unable to load properties file " + configFile);
		}

		return properties;
	}

	/**
	 * Validates the specified properties contain all required properties.
	 * If any are missing they are returned in a list.
	 *
	 * @param properties the properties to test
	 * @return a ArrayList<String> of missing properties
	 */
	protected static ArrayList<String> checkProperties(Properties properties) {
		ArrayList<String> missingProperties = new ArrayList<String>();

		if (!properties.containsKey("bootstrap.servers")) {
			missingProperties.add("bootstrap.servers");
		}

		if (!properties.containsKey("port")) {
			missingProperties.add("port");
		}

		return missingProperties;
	}

	/**
	 * Given String[] args, return the path and configuration file specified on the commandline.
	 *
	 * @param args the commandline args passed in
	 * @return a String containing the path and name of the configuration file
	 */
	protected static String getConfig(String[] args) {
		Options options = new Options();

		Option input = new Option("c", "config", true, "location and name of the config file");
		input.setRequired(true);
		options.addOption(input);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();

		String configFile = null;
		try {
			CommandLine cli = parser.parse(options, args);
			configFile = cli.getOptionValue("config");
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("ConsumerGraph", options);

			System.exit(1);
		}

		return configFile;
	}

	public static void main(String[] args) {
		try {
			Properties properties = loadProperties(getConfig(args));
			LOGGER.info("Properties: " + properties.toString());

			ArrayList<String> missingProperties = checkProperties(properties);
			if (missingProperties.size() > 0) {
				LOGGER.error("Missing required properties: " + missingProperties.toString());
				System.exit(1);
			}

			if (isKafkaAlive(properties.getProperty(BOOTSTRAP_SERVERS))) {
				new ConsumerGraphServer(properties);
			} else {
				LOGGER.error("Kafka is not reachable, bootstrap.servers: " + properties.getProperty(BOOTSTRAP_SERVERS));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

