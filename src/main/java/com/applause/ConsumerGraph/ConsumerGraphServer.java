package com.applause.ConsumerGraph;

import freemarker.template.*;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;


/**
 * The webserver. This serves the UI, including templating values, and initialization of the Kafka consumer.
 */
public class ConsumerGraphServer extends AbstractHandler {
	private static Logger LOGGER = LoggerFactory.getLogger(ConsumerGraphServer.class);

	private TopicConsumerMapper consumer;
	private Template indexTemplate;

	private String topicFilter;
	private String consumerFilter;
	private String clusterName;
	private String bootstrapServers;
	private String uiStyle;
	private long timeout;
	private int port;

	/**
	 * Create and start the Kafka consumer and webserver based on the passed in properties.
	 *
	 * @param properties the properties for this server
	 * @throws Exception
	 */
	public ConsumerGraphServer(Properties properties) throws Exception {
		try {
			this.bootstrapServers = properties.getProperty(ConsumerGraph.BOOTSTRAP_SERVERS);
			this.topicFilter = properties.getProperty(ConsumerGraph.FILTERS_TOPIC);
			this.consumerFilter = properties.getProperty(ConsumerGraph.FILTERS_CONSUMER);
			this.clusterName = properties.getProperty(ConsumerGraph.CLUSTER_NAME) == null ? ConsumerGraph.DEFAULT_CLUSTER_NAME : properties.getProperty(ConsumerGraph.CLUSTER_NAME);
			this.uiStyle = properties.getProperty(ConsumerGraph.UI_STYLE) == null ? ConsumerGraph.DEFAULT_UI_STYLE : properties.getProperty(ConsumerGraph.UI_STYLE);
			this.timeout = Long.parseLong(properties.getProperty(ConsumerGraph.TIMEOUT));
			this.port = Integer.parseInt(properties.getProperty(ConsumerGraph.PORT));

			createTopicConsumerMapper();

			loadTemplate();

			initServer(this.port);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	@Override
	public void handle(String target,
					   Request baseRequest,
					   HttpServletRequest request,
					   HttpServletResponse response) throws IOException,
			ServletException {
		try {
			response.setContentType("text/html; charset=utf-8");
			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().print(getContent());
		} catch (TemplateException te) {
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			te.printStackTrace();
			response.getWriter().print(":(");
		}

		baseRequest.setHandled(true);
	}

	/**
	 * Prepare the freemarker template for loading.
	 * Set base path, encoding, locale and exception handling, then set the template.
	 *
	 * @throws IOException
	 */
	private void loadTemplate() throws IOException {
		Configuration config = new Configuration();
		config.setClassForTemplateLoading(this.getClass(), "/templates");

		// recommended settings
		config.setIncompatibleImprovements(new Version(2, 3, 20));
		config.setDefaultEncoding("UTF-8");
		config.setLocale(Locale.US);

		config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

		setTemplate(config);
	}

	/**
	 * Set the template based on the passed-in configuration.
	 *
	 * @param config the configuration containing the type of template to use
	 * @throws IOException
	 */
	private void setTemplate(Configuration config) throws IOException {
		if (this.uiStyle.equalsIgnoreCase("tree")) {
			this.indexTemplate = config.getTemplate("tree.ftl");
		} else {
			this.indexTemplate = config.getTemplate("graph.ftl");
		}
	}

	/**
	 * Given a map of JSONArray, populate the freemarker template.
	 *
	 * @param map a JSONArray detailing the mappings between Kafka topics and consumers
	 * @return a String containing the fully-templated text
	 * @throws IOException
	 * @throws TemplateException
	 */
	private String populateTemplate(JSONArray map) throws IOException, TemplateException {
		Map<String, Object> input = new HashMap<String, Object>();

		// TODO: set data-last-refreshed time
		input.put("cluster_name", this.clusterName);
		input.put("links", map);

		StringWriter sw = new StringWriter();
		indexTemplate.process(input, sw);
		return sw.toString();
	}

	/**
	 * Fetch the latest Kafka topic-to-consumer map, build a JSONArray based on the data,
	 * template the data and return that String.
	 *
	 * @return a String containing the fully-templated text.
	 * @throws IOException
	 * @throws TemplateException
	 */
	private String getContent() throws IOException, TemplateException {
		JSONArray map = new JSONArray();
		HashMap<String, ArrayList<String>> tcm = this.consumer.getTopicConsumerGroupMap();
		for (String topic : tcm.keySet()) {
			JSONObject oneTopic = new JSONObject();
			oneTopic.put("name", topic);
			oneTopic.put("parent", this.clusterName);
			JSONArray oneChildlist = new JSONArray();
			for (String group : tcm.get(topic)) {
				JSONObject oneMap = new JSONObject();
				oneMap.put("name", group);
				oneMap.put("parent", topic);
				oneChildlist.put(oneMap);
			}
			oneTopic.put("children", oneChildlist);
			map.put(oneTopic);
		}

		return populateTemplate(map);
	}

	/**
	 * Create the Kafka consumer that maps topics to consumers.
	 * This runs in a background thread.
	 */
	private void createTopicConsumerMapper() {
		this.consumer = new TopicConsumerMapper(this.bootstrapServers, this.topicFilter, this.consumerFilter, this.timeout);

		Thread thread = new Thread(this.consumer);

		thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
			public void uncaughtException(Thread t, Throwable e) {
				e.printStackTrace();
				System.exit(1);
			}
		});

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				consumer.shutdown();
			}
		});

		thread.start();
	}

	/**
	 * Initialize the webserver.
	 *
	 * @param port the port the server should listen on
	 */
	private void initServer(int port) {
		try {
			Server server = new Server(port);

			server.setHandler(this);

			server.start();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
