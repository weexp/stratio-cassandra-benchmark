package com.stratio.cassandra.benchmark;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class Client implements Runnable {

	private static final Logger logger = Logger.getLogger("benchmark");

	private Session session;
	private String keyspace;
	private String table;
	private String column;
	private String clause;
	private int limit;
	private Stats stats;

	public Client(Session session,
	              String keyspace,
	              String table,
	              String column,
	              String clause,
	              int limit,
	              Stats stats,
	              Dataset dataset) {
		this.session = session;
		this.keyspace = keyspace;
		this.table = table;
		this.column = column;
		this.clause = clause;
		this.limit = limit;
		this.stats = stats;
	}

	public void run() {
		try {

			Select select = QueryBuilder.select()
			                            .from(keyspace, table)
			                            .where(QueryBuilder.eq(column, clause))
			                            .limit(limit);

			long startTime = System.currentTimeMillis();

			ResultSet rs = session.execute(select);
			List<Row> rows = rs.all();

			long queryTime = System.currentTimeMillis() - startTime;

			stats.inc(queryTime);
			logger.debug("QUERY : " + select.toString() + " " + queryTime + " ms");

			for (Row row : rows) {
				logger.debug("\tROW : " + row);
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
	}

}
