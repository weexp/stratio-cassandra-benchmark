package com.stratio.cassandra.benchmark;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.util.concurrent.RateLimiter;

public class Client implements Runnable {

	private static final Logger logger = Logger.getLogger("benchmark");

	private Session session;
	private Stats stats;
	private List<String> dataset;
	private String keyspace;
	private String table;
	private String column;
	private Boolean relevance;

	private Double rate;
	private Integer limit;

	public Client(Session session,
	              Stats stats,
	              List<String> dataset,
	              Double rate,
	              Integer limit,
	              String keyspace,
	              String table,
	              String column,
	              Boolean relevance) {

		this.session = session;
		this.stats = stats;
		this.limit = limit;
		this.keyspace = keyspace;
		this.table = table;
		this.column = column;
		this.relevance = relevance;
		this.dataset = dataset;
		this.rate = rate;
	}

	public void run() {
		try {

			RateLimiter rateLimiter = RateLimiter.create(rate);

			for (String data : dataset) {

				rateLimiter.acquire();

				String clause = String.format("{%s : {type : \"lucene\", default_field : \"%s\", query : \"%s\"}}",
				                              relevance ? "query" : "filter",
				                              column,
				                              data);
				Select select = QueryBuilder.select()
				                            .from(keyspace, table)
				                            .where(QueryBuilder.eq(column, clause))
				                            .limit(limit);
				long startTime = System.currentTimeMillis();

				ResultSet rs = session.execute(select.setConsistencyLevel(ConsistencyLevel.ONE));
				List<Row> rows = rs.all();

				long queryTime = System.currentTimeMillis() - startTime;

				stats.inc(queryTime);
				logger.debug("QUERY : " + select.toString() + " " + queryTime + " ms");

				for (Row row : rows) {
					logger.debug("\tROW : " + row);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
	}

}
