package com.semantico.cassandra.fs.tests;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.cassandra.contrib.fs.CassandraFacade;
import org.junit.Test;

public class CassandraFacadeTest extends AbstractCassandraFsTest {
	
	@Test
	public void configExistsTest() throws IOException {
		assertNotNull(CassandraFacade.getInstance(null));
	}
	
	@Test(expected=IOException.class)
	public void keyspaceDroppedTest() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		facade.delete("bla");
	}
	
	@Test(expected=IOException.class)
	public void incorrectColumnPathTest() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		facade.get("foo", "bar");
	}
	
	@Test()
	public void listFileTest() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		assertNotNull(facade.listFile("someFile"));
	}

}
