package com.stratio.cassandra.benchmark;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.RateLimiter;

public class Client implements Runnable {

	private static final Logger logger = Logger.getLogger("benchmark");

	private Session session;
	private PreparedStatement ps;
	private RateLimiter rateLimiter;
	private Stats stats;
	private String column = null;
	private Integer queries = null;
	private Dataset dataset;
	private Boolean relevance;

	public Client(Session session,PreparedStatement ps,
	              RateLimiter rateLimiter,
	              Stats stats,
	              Dataset dataset,
	              String column,
	              Integer queries,
	              Boolean relevance) {
		this.session = session;
		this.ps = ps;
		this.rateLimiter = rateLimiter;
		this.stats = stats;
		this.column = column;
		this.queries = queries;
		this.dataset = dataset;
		this.relevance = relevance;
	}

	public void run() {
		try {


		for (String data : dataset.get(queries)) {
			rateLimiter.acquire();
			String clause = String.format("{%s : {type : \"lucene\", default_field : \"%s\", query : \"%s\"}}",
			                              relevance ? "query" : "filter",
			                              column,
			                              data);
			long startTime = System.currentTimeMillis();

			ResultSet rs = session.execute(ps.bind(clause));
			List<Row> rows = rs.all();

			long queryTime = System.currentTimeMillis() - startTime;
			stats.inc(queryTime);
			logger.debug("QUERY : " + ps.getQueryString() + " " + queryTime + " ms");

			// for (Row row : rows) {
			// logger.debug("\tROW : " + row);
			// }

		}
		}catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
	}

}
