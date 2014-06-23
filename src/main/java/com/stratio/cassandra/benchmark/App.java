package com.stratio.cassandra.benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.log4j.chainsaw.Main;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.RateLimiter;
import com.stratio.cassandra.index.util.Log;

/**
 * Hello world!
 * 
 */
public class App {

	private static final Logger logger = Logger.getLogger("benchmark");

	public static void main(String[] args) throws IOException, InterruptedException {

		logger.info("STARTING");

		Dataset dataset = null;
		Properties properties = null;

		Options options = new Options();
		options.addOption("d", "dataset", true, "Dataset file");
		options.addOption("p", "properties", true, "Properties file");
		options.addOption("h", "help", false, "Prints help");

		try {

			// Build command line parser
			CommandLineParser parser = new BasicParser();
			CommandLine cmdLine = parser.parse(options, args);

			// Print help
			if (cmdLine.hasOption("h")) {
				new HelpFormatter().printHelp(Main.class.getCanonicalName(), options);
				System.exit(1);
			}

			if (cmdLine.hasOption("dataset")) {
				String path = cmdLine.getOptionValue("dataset");
				dataset = new Dataset(path);
			} else {
				System.out.println("Dataset path option required");
				System.exit(-1);
			}

			if (cmdLine.hasOption("properties")) {
				String path = cmdLine.getOptionValue("properties");
				InputStream is = new FileInputStream(path);
				properties = new Properties();
				properties.load(is);
				is.close();
			} else {
				System.out.println("Properties path option required");
				System.exit(-1);
			}

		} catch (org.apache.commons.cli.ParseException e) {
			new HelpFormatter().printHelp(Main.class.getCanonicalName(), options);
			System.exit(-1);
		}

		String hosts = properties.getProperty("hosts");
		String keyspace = properties.getProperty("keyspace");
		String table = properties.getProperty("table");
		String column = properties.getProperty("column");
		Double rate = Double.parseDouble(properties.getProperty("rate"));
		Integer threads = Integer.parseInt(properties.getProperty("threads"));
		Integer queries = Integer.parseInt(properties.getProperty("queries"));
		Integer limit = Integer.parseInt(properties.getProperty("limit"));
		Boolean relevance = Boolean.parseBoolean(properties.getProperty("relevance"));

		Cluster cluster = Cluster.builder().addContactPoint(hosts).build();
		cluster.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM);
		logger.debug("Connected to cluster (" + hosts + "): " + cluster.getMetadata().getClusterName() + "\n");
		Session session = cluster.connect();

		String query = String.format("SELECT * FROM %s.%s WHERE %s=? LIMIT %d;", keyspace, table, column, limit);
		PreparedStatement ps = session.prepare(query);

		RateLimiter rateLimiter = RateLimiter.create(rate);
		Stats stats = new Stats();

		ExecutorService executorService = Executors.newFixedThreadPool(threads);
		for (int i = 0; i < threads; i++) {
			Client client = new Client(session, ps, rateLimiter, stats, dataset, column, queries, relevance);
			executorService.execute(client);
		}
		executorService.shutdown();
		executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

		Log.info(stats.toString());

		session.close();

		System.exit(1);
	}
}
