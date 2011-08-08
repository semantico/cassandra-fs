package org.apache.cassandra.contrib.fs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.log4j.Logger;
import me.prettyprint.cassandra.connection.ConcurrentHClientPool;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.PoolExhaustedException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SubColumnQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

public class CassandraFacade {

	private static Logger LOGGER = Logger.getLogger(CassandraFacade.class);
	private static CassandraFacade instance;

	public static CassandraFacade getInstance(Configuration conf) throws IOException {
		if (instance == null) {
			synchronized (CassandraFacade.class) {
				if (instance == null) {
					if(conf == null) { //no custom config, initialize from file
						File clientConfFile = new File(System.getProperty("storage-config")+ File.separator + "config.properties");
						if (!clientConfFile.exists()) {
							throw new RuntimeException("'" + clientConfFile.getAbsolutePath()+ "' does not exist!");
						}
						conf = new Configuration(clientConfFile.getAbsolutePath());
					}
					instance = new CassandraFacade(conf);
				}
			}
		}
		return instance;
	}

	public static void dropInstance() {
		instance = null;
	}

	private Cluster cluster;
	private Configuration conf;
	private final StringSerializer stringSerializer = StringSerializer.get();
	private final BytesArraySerializer byteSerializer = BytesArraySerializer.get();

	private CassandraFacade(Configuration conf) throws IOException {
		this.conf = conf;
		cluster = HFactory.getOrCreateCluster(conf.getClusterName(), new CassandraHostConfigurator(conf.getHosts()));

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

		KeyspaceDefinition kspDescrip = cluster.describeKeyspace(conf.getKeyspace());
		KeyspaceDefinition kspDef = HFactory.createKeyspaceDefinition(conf.getKeyspace(), conf.getReplicaPlacementStrategy(), conf.getReplicationFactor(), cfDefs);
		if(kspDescrip == null) {
			cluster.addKeyspace(kspDef);
		} else {
			List<ColumnFamilyDefinition> cfs =kspDescrip.getCfDefs();
			boolean hasFileFamily = false;
			boolean hasFolderFamily = false;
			for(ColumnFamilyDefinition cfDef : cfs) {
				if(cfDef.getName().equals(FSConstants.DefaultFileCF)) {
					hasFileFamily = true;
				} else if(cfDef.getName().equals(FSConstants.DefaultFolderCF)) {
					hasFolderFamily = true;
				}
			}
			if(!hasFileFamily || !hasFolderFamily) {
				throw new RuntimeException("Incorrectly Configured Keyspace detected");
			}
		}
	}

	public Configuration getConf() {
		return conf;
	}

	private ColumnPath extractColumnPath(String column) throws IOException {
		String[] subColumns = column.split(":");
		ColumnPath columnPath = new ColumnPath();
		columnPath.setColumn_family(subColumns[0]);
		if (subColumns.length == 2) {
			columnPath.unsetSuper_column();
			columnPath.setColumn(subColumns[1].getBytes());
		} else if (subColumns.length == 3) {
			columnPath.setSuper_column(subColumns[1].getBytes());
			columnPath.setColumn(subColumns[2].getBytes());
		} else {
			throw new IOException("The column is not the right format:" + column);
		}
		return columnPath;
	}

	public void put(String key, String column, byte[] value) throws IOException {
		LOGGER.trace("Hitting put");
		try {
			ColumnPath columnPath = extractColumnPath(column);
			Mutator<String> mutator = HFactory.createMutator(HFactory.createKeyspace(conf.getKeyspace(), cluster), stringSerializer);

			if(columnPath.isSetSuper_column()) {
				LOGGER.trace("- put into supercolumn of " + columnPath.getColumn_family());
				HColumn<String, byte[]> col = HFactory.createColumn(new String(columnPath.column.array()), value, stringSerializer, byteSerializer);
				ArrayList<HColumn<String, byte[]>> cols = new ArrayList<HColumn<String, byte[]>>();
				cols.add(col);
				HSuperColumn<String, String, byte[]> superCol = HFactory.createSuperColumn(new String(columnPath.super_column.array()), cols, stringSerializer, stringSerializer, byteSerializer);
				LOGGER.trace("- - supercolumn : name:"+ new String(columnPath.super_column.array()) + " key:" + key );
				LOGGER.trace("- - - column : name:"+ new String(columnPath.column.array()) + " value:" + new String(value));
				mutator.insert(key, columnPath.getColumn_family(), superCol);
			} else {
				LOGGER.trace("- put into column " + columnPath.getColumn_family());
				HColumn<String, byte[]> col = HFactory.createColumn(new String(columnPath.column.array()), value, stringSerializer, byteSerializer);
				mutator.insert(key, columnPath.getColumn_family(), col);
				LOGGER.trace("- - column : name:"+ new String(columnPath.column.array()) + " key:" + key+" value:" + new String(value));
			}

		} catch (Exception e) {
			throw new IOException(e);
		} 
	}

	public void batchPut(String key, String cfName, String colName, Map<byte[], byte[]> map, boolean isSuperColumn) throws IOException {
		LOGGER.trace("Hitting batchPut");
		try {
			Keyspace ks = HFactory.createKeyspace(conf.getKeyspace(), cluster);
			Mutator<String> mutator = HFactory.createMutator(HFactory.createKeyspace(conf.getKeyspace(), cluster), stringSerializer);

			if (!isSuperColumn) {
				LOGGER.trace("- put into column of " + cfName);
				for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
					HColumn<String, String> col = HFactory.createColumn(new String(entry.getKey()), new String(entry.getValue()), stringSerializer, stringSerializer);
					mutator.addInsertion(key, cfName, col);
					LOGGER.trace("- - column : name:"+ new String(entry.getKey() + " key:" + key+" value:" + new String(entry.getValue())));
				}
			}else{
				LOGGER.trace("- put into supercolumn of " + cfName);
				List<HColumn<String, byte[]>> cols = new ArrayList<HColumn<String, byte[]>>();
				LOGGER.trace("- - supercolumn : name:"+ colName + " key:" + key );
				for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
					HColumn<String, byte[]> col = HFactory.createColumn(new String(entry.getKey()), entry.getValue(), stringSerializer, byteSerializer);
					cols.add(col);
					LOGGER.trace("- - - column : name:"+ new String(entry.getKey()) + " value:" + new String(entry.getValue()));
				}
				HSuperColumn<String , String, byte[]> superCol = HFactory.createSuperColumn(colName, cols, stringSerializer, stringSerializer, byteSerializer);
				mutator.addInsertion(key, cfName, superCol);
			}

			//ks.batchInsert(key, cfMap, superCFMap);
			MutationResult result = mutator.execute();
		} catch (Exception e) {
			throw new IOException(e);
		} 
	}

	public byte[] get(String key, String column) throws IOException {
		Keyspace ks = null;
		LOGGER.info("Hitting Get: Key:" + key + "  ColumnPath:"  + column);
		try {
			ColumnPath columnPath = extractColumnPath(column);
			ks = HFactory.createKeyspace(conf.getKeyspace(), cluster);
			if (columnPath.isSetSuper_column() && columnPath.isSetColumn()){
				SubColumnQuery<String, String, String, byte[]> query = HFactory.createSubColumnQuery(ks, stringSerializer, stringSerializer, stringSerializer, byteSerializer);
				query.setColumnFamily(columnPath.getColumn_family());
				query.setSuperColumn(new String(columnPath.getSuper_column()));
				query.setColumn(new String(columnPath.getColumn()));
				query.setKey(key);
				QueryResult<HColumn<String, byte[]>> result = query.execute();
				HColumn<String, byte[]> col = result.get();
				if(col == null) {
					throw new NotFoundException();
				}
				return col.getValue();
			} else if(columnPath.isSetColumn()) {
				ColumnQuery<String, String, byte[]> query = HFactory.createColumnQuery(ks, stringSerializer, stringSerializer, byteSerializer);
				query.setColumnFamily(columnPath.getColumn_family());
				query.setName(new String(columnPath.getColumn()));
				query.setKey(key);
				QueryResult<HColumn<String, byte[]>>  result = query.execute();
				HColumn<String, byte[]>  col = result.get();
				if(col == null) {
					throw new NotFoundException();
				}
				return col.getValue();
			}
		} catch (Exception e) {
			//e.printStackTrace();
			throw new IOException(e);
		}
		//Error, no column specified
		throw new IllegalArgumentException("Cannot Query without a column");
	}

	public void delete(String key) throws IOException {
		Keyspace ks = null;
		try {
			ks = HFactory.createKeyspace(conf.getKeyspace(), cluster);
			Mutator<String> mutator = HFactory.createMutator(ks, stringSerializer);
			mutator.addDeletion(key, FSConstants.DefaultFileCF);
			mutator.addDeletion(key, FSConstants.DefaultFolderCF);
			mutator.execute();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public void delete(String key, String columnFamily, String superColumn) throws IOException {
		Keyspace ks = null;
		try {
			ks = HFactory.createKeyspace(conf.getKeyspace(), cluster);
			Mutator<String> mutator = HFactory.createMutator(ks, stringSerializer);
			mutator.addSuperDelete(key, columnFamily, superColumn, stringSerializer);
			mutator.execute();

		} catch (Exception e) {
			throw new IOException(e);
		} 
	}

	public boolean exist(String key) throws IOException {
		Keyspace ks = null;
		try {
			ks = HFactory.createKeyspace(conf.getKeyspace(), cluster);

			SubColumnQuery<String, String, String, byte[]> query = HFactory.createSubColumnQuery(ks, stringSerializer, stringSerializer, stringSerializer, byteSerializer);
			query.setColumnFamily(FSConstants.DefaultFolderCF);
			query.setSuperColumn(FSConstants.DefaultFolderFlag);
			query.setColumn(FSConstants.TypeAttr);
			query.setKey(key);
			QueryResult<HColumn<String, byte[]>> result = query.execute();
			if(result == null || result.get() == null ) {
				throw new NotFoundException();
			}
			//TODO: what does this do ? if we dont use the result in any way unless an exception is thrown
		} catch (NotFoundException e) {
			// do nothing
		} catch (IllegalArgumentException e) {
			throw new IOException(e);
		} catch (IllegalStateException e) {
			throw new IOException(e);
		} catch (PoolExhaustedException e) {
			throw new IOException(e);
		} catch (Exception e) {
			throw new IOException(e);
		} 

		try {
			ks = HFactory.createKeyspace(conf.getKeyspace(), cluster);
			ColumnQuery<String, String, byte[]> query = HFactory.createColumnQuery(ks, stringSerializer, stringSerializer, byteSerializer);
			query.setColumnFamily(FSConstants.DefaultFileCF);
			query.setName(FSConstants.ContentAttr + "_0");
			query.setKey(key);
			QueryResult<HColumn<String, byte[]>> result = query.execute();
			if(result == null || result.get() == null ) {
				throw new NotFoundException();
			}
			return true;
		} catch (NotFoundException e) {
			// do nothing
		} catch (IllegalArgumentException e) {
			throw new IOException(e);
		} catch (IllegalStateException e) {
			throw new IOException(e);
		} catch (PoolExhaustedException e) {
			throw new IOException(e);
		} catch (Exception e) {
			throw new IOException(e);
		}
		return false;
	}


	public List<Path> list(String key, String columnFamily, boolean includeFolderFlag) throws IOException {
		Keyspace ks = null;
		try {
			List<Path> children = new ArrayList<Path>();
			ks = HFactory.createKeyspace(conf.getKeyspace(), cluster);

			if (columnFamily.equals(FSConstants.DefaultFolderCF)) {
				SuperSliceQuery<String, String, String, byte[]> query = HFactory.createSuperSliceQuery(ks, stringSerializer, stringSerializer, stringSerializer, byteSerializer);
				query.setColumnFamily(columnFamily);
				//query.setColumnNames(columnNames); //TODO: ???
				query.setRange("", "", false, Integer.MAX_VALUE); //TODO: maybe convert all the supercolumn names to byte[] ? or not
				query.setKey(key);
				List<HSuperColumn<String, String, byte[]>> superCols = query.execute().get().getSuperColumns();
				for(HSuperColumn<String, String, byte[]> sc : superCols) {
					String name = sc.getName();
					List<HColumn<String, byte[]>> attributes = sc.getColumns();
					Path path = new Path(name, attributes); 
					if (includeFolderFlag) {
						children.add(path);
					} else if (!name.equals(FSConstants.DefaultFolderFlag)) {
						children.add(path);
					}
				}
			} else if (columnFamily.equals(FSConstants.DefaultFileCF)) {
				SliceQuery<String, String, byte[]> query = HFactory.createSliceQuery(ks, stringSerializer, stringSerializer, byteSerializer);
				query.setColumnFamily(columnFamily);
				query.setKey(key);
				query.setRange("", "", false, Integer.MAX_VALUE);
				List<HColumn<String, byte[]>> slice = query.execute().get().getColumns();
				if (slice.size() > 0) {
					Path path = new Path(key, slice); 
					children.add(path);
				}
			} else {
				throw new RuntimeException("Do not support CF:'" + columnFamily + "' now");
			}
			return children;
		} catch (Exception e) {
			throw new IOException(e);
		} 
	}

	public Path listFile(String file) throws IOException {
		Keyspace ks = null;
		try {
			ks = HFactory.createKeyspace(conf.getKeyspace(), cluster);
			SliceQuery<String, String, byte[]> query = HFactory.createSliceQuery(ks, stringSerializer, stringSerializer, byteSerializer);
			query.setColumnFamily(FSConstants.DefaultFileCF);
			query.setKey(file);
			query.setRange("", "", false, Integer.MAX_VALUE);
			List<HColumn<String, byte[]>> columns = query.execute().get().getColumns();
			return new Path(file, columns);
		} catch (Exception e) {
			throw new IOException(e);
		} 
	}
}
