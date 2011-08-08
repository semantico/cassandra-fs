package com.semantico.cassandra.fs.tests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.contrib.fs.CassandraFacade;
import org.apache.cassandra.contrib.fs.FSConstants;
import org.junit.*;

import static org.junit.Assert.*;

public class CassandraFacadeTest extends AbstractCassandraFsTest {
	
	@Before
	public void cleanKeyspace() throws FileNotFoundException, IOException {
		// each method in this class must set up the keyspace
		super.cleanPrebuiltKeyspace(); // drop the keyspace & singletons
		System.setProperty("storage-config", "src/test/resources/");
	}
	
	@Test
	public void defaultKeyspaceSetupTest() throws IOException {
		assertNotNull(CassandraFacade.getInstance(null));
	}
	
	@Test
	public void defaultIncorrectPathKeyspaceSetupTest() throws IOException {
		System.setProperty("storage-config", "nonExistantPath");
		boolean wasThrown = false;
		try {
		CassandraFacade.getInstance(null);
		} catch (RuntimeException e) {
			if(e.getMessage().endsWith("' does not exist!")); {
				wasThrown = true;
			}
		}
		assertTrue("Didnt Complain that the config file didnt exist", wasThrown);
	}
	
	@Test
	public void nonConflictingDefaultKeyspaceSetupTest() throws IOException {
		setupKeyspace();
		assertNotNull(CassandraFacade.getInstance(conf));
	}
	
	@Test
	public void conflictingDefaultKeyspaceSetupTest() throws IOException {
		setupPartialKeyspace(); // only half setup
		System.out.println(cluster.describeKeyspace(conf.getKeyspace()));
		boolean wasThrown = false;
		try {
			CassandraFacade.getInstance(conf);
			} catch (RuntimeException e) {
				if(e.getMessage().equals("Incorrectly Configured Keyspace detected")); {
					wasThrown = true;
				}
			}
			assertTrue("Didnt Complain that the keyspace existed but didnt contain the right tables", wasThrown);
	}
	
	private void setupKeyspace() {
		List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();

		{//FILE COLUMN FAMILY
			ColumnFamilyDefinition fileCfDef = HFactory.createColumnFamilyDefinition(conf.getKeyspace(), FSConstants.DefaultFileCF, ComparatorType.BYTESTYPE);
			cfDefs.add(fileCfDef);
		} 
		{ //FOLDER COLUMN FAMILY
			ThriftCfDef  folderCfDef = (ThriftCfDef) HFactory.createColumnFamilyDefinition(conf.getKeyspace(), FSConstants.DefaultFolderCF, ComparatorType.UTF8TYPE);
			//Need to downcast to Thrift in order to make a superColumnFamily definition. This is a known problem, but hector isnt being maintained officially anymore
			folderCfDef.setColumnType(ColumnType.SUPER);
			cfDefs.add(folderCfDef);
		}
		
		KeyspaceDefinition kspDef = HFactory.createKeyspaceDefinition(conf.getKeyspace(), conf.getReplicaPlacementStrategy(), conf.getReplicationFactor(), cfDefs);
		cluster.addKeyspace(kspDef);
	}
	
	private void setupPartialKeyspace() {
		List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();

		{//FILE COLUMN FAMILY
			ColumnFamilyDefinition fileCfDef = HFactory.createColumnFamilyDefinition(conf.getKeyspace(), FSConstants.DefaultFileCF, ComparatorType.BYTESTYPE);
			cfDefs.add(fileCfDef);
		}
		KeyspaceDefinition kspDef = HFactory.createKeyspaceDefinition(conf.getKeyspace(), conf.getReplicaPlacementStrategy(), conf.getReplicationFactor(), cfDefs);
		cluster.addKeyspace(kspDef);
	}

}
