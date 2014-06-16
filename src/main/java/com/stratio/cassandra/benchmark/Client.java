package com.stratio.cassandra.benchmark;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.RateLimiter;

public class Client implements Runnable {

	private static final Logger logger = Logger.getLogger("benchmark");

	private RateLimiter rateLimiter;
	private Stats stats;
	private String keyspace = null;
	private String table = null;
	private String column = null;
	private Integer queries = null;
	private Integer limit = null;
	private Dataset dataset;
	private Session session;

	public Client(Session session,
	              RateLimiter rateLimiter,
	              Stats stats,
	              Dataset dataset,
	              String keyspace,
	              String table,
	              String column,
	              Integer queries,
	              Integer limit) {
		this.session = session;
		this.rateLimiter = rateLimiter;
		this.stats = stats;
		this.keyspace = keyspace;
		this.table = table;
		this.column = column;
		this.queries = queries;
		this.limit = limit;
		this.dataset = dataset;
	}

	public void run() {

		for (String data : dataset.get(queries)) {
			rateLimiter.acquire();
			String clause = String.format("{query : {type : \"%s\", default_field : \"lucene\", query : \"%s\"}}",
			                              column,
			                              data);
			long startTime = System.currentTimeMillis();
			BuiltStatement statement = QueryBuilder.select()
			                                       .from(keyspace, table)
			                                       .where(QueryBuilder.eq(column, clause))
			                                       .limit(limit);
			ResultSet rs = session.execute(statement);
			List<Row> rows = rs.all();

			long queryTime = System.currentTimeMillis() - startTime;
			stats.inc(queryTime);
			logger.debug("QUERY : " + statement + " " + queryTime + " ms");

			// for (Row row : rows) {
			// logger.debug("\tROW : " + row);
			// }

		}
	}

}
