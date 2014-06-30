package com.stratio.cassandra.benchmark;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.RateLimiter;

public class Client implements Runnable {

	private static final Logger logger = Logger.getLogger("benchmark");

	private Session session;
	private BoundStatement bs;
	private RateLimiter rateLimiter;
	private Stats stats;

	public Client(Session session, BoundStatement bs, RateLimiter rateLimiter, Stats stats, Dataset dataset) {
		this.session = session;
		this.bs = bs;
		this.rateLimiter = rateLimiter;
		this.stats = stats;
	}

	public void run() {
		try {
			rateLimiter.acquire();

			long startTime = System.currentTimeMillis();

			ResultSet rs = session.execute(bs);
			List<Row> rows = rs.all();

			long queryTime = System.currentTimeMillis() - startTime;
			
			stats.inc(queryTime);
			logger.debug("QUERY : " + bs.toString() + " " + queryTime + " ms");

			for (Row row : rows) {
				logger.debug("\tROW : " + row);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
	}

}
