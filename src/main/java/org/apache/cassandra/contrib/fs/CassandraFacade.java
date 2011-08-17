package org.apache.cassandra.contrib.fs;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.log4j.Logger;
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
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SubColumnQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

/**
 * CassandraFacade is a singleton that encapulates access to the cassandra database.
 * It will attempt to configure access to the database from a provided Configuration object or 
 * look in the directory specified by the system property 'storage-config' if none is provided,
 * as a last resort it will scan the user directory for the file. Configuration files must be
 * named 'config.properties'. A runtime exception will occour if a configuration is not supplied
 * See the supplied configuration file for further information on configuration
 * @author Edd King
 *
 */
public class CassandraFacade {

	private static Logger LOGGER = Logger.getLogger(CassandraFacade.class);
	private static CassandraFacade instance;

	/**
	 * Gets the instance of this singleton class.
	 * Creates a new instance with the configuration provided if no instance currently exists.
	 * Note: singletons are not shared between class loaders. 
	 * @param conf a configuration object to use
	 * @return The singleton instance of CassandraFacade
	 * @throws IOException when an error occours while reading a configuration file
	 */
	public static CassandraFacade getInstance(Configuration conf) throws IOException {
		if (instance == null) {
			synchronized (CassandraFacade.class) {
				if (instance == null) {
					if(conf == null) { //no custom config, initialize from file
						String configLocation = System.getProperty("storage-config");
						URL config;
						if(configLocation == null) {
						config = CassandraFacade.class.getClassLoader().getResource("config.properties");
						} else {
							//weirdly sensitive to the number of / after file:
							config = new URL("file:////" +System.getProperty("user.dir")+ File.separator+ configLocation+File.separator+ "config.properties");
						}
						File clientConfFile = null;
						try {
							clientConfFile = new File(config.toURI());
						} catch (URISyntaxException e) {
							//e.printStackTrace();
						}
						if (clientConfFile == null || !clientConfFile.exists()) {
							throw new RuntimeException("'" + config.toString()+ "' does not exist!");
						}
						conf = new Configuration(clientConfFile.getAbsolutePath());
					}
					instance = new CassandraFacade(conf);
				}
			}
		}
		return instance;
	}

	/**
	 * <b>This Method breaks the singleton pattern.
	 * </b>
	 * When next an instance of the Facade is requested, a new one will be created. This method should only be used if for some
	 * reason you need to re-configure. It is the responsibility of the user to manage references to other instances of the facade,
	 * (The CassandraFileSystem singleton must be dropped too)
	 */
	public static void dropInstance() {
		instance = null;
	}

	private Cluster cluster;
	private Configuration conf;
	private Keyspace ksp;
	private final StringSerializer stringSerializer = StringSerializer.get();
	private final BytesArraySerializer byteSerializer = BytesArraySerializer.get();

	private CassandraFacade(Configuration conf) throws IOException {
		this.conf = conf;
		CassandraHostConfigurator hostConf = new CassandraHostConfigurator(conf.getHosts());
		if(conf.getBufferedMemoryLimit() != -1) {
			if(conf.isSetMaxActive() || conf.isSetMaxIdle()) {
				LOGGER.warn("Both max active/idle and memory limit configurations have been set. Memory limit is being used, max active/idle will be ignored");
			}
			conf.setMaxActiveFromMemoryLimit();
			conf.setMaxIdleFromMaxActive();
			hostConf.setMaxActive(conf.getMaxActive());
			hostConf.setMaxIdle(conf.getMaxIdle());
		} else {
			if(conf.getMaxActive() < 1) {
				throw new IOException("Max Active cannot be set below 1");
			}
			hostConf.setMaxActive(conf.getMaxActive());
			hostConf.setMaxIdle(conf.getMaxIdle());
			conf.setMemoryLimitFromMaxActive();
		}
		
		hostConf.setMaxWaitTimeWhenExhausted(conf.getMaxWaitTimeWhenExhausted());
		hostConf.setCassandraThriftSocketTimeout(conf.getCassandraThriftSocketTimeout());
		cluster = HFactory.getOrCreateCluster(conf.getClusterName(), hostConf);

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
		ksp = HFactory.createKeyspace(conf.getKeyspace(), cluster);
	}

	/**
	 * Gets the configuration object used by this facade
	 * @return the configuration object used by this facade
	 */
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

	/**
	 * Puts bytes into the specified key & column.
	 * @param key The key to add the column to
	 * @param column the path to the column e.g. ColumnFamily:SuperColumn:SubColumn or ColumnFamily:Column
	 * @param value the bytes to put in the column
	 * @throws IOException if any error occours while attempting to perform this action
	 */
	public void put(String key, String column, byte[] value) throws IOException {
		LOGGER.trace("Hitting put");
		try {
			ColumnPath columnPath = extractColumnPath(column);
			Mutator<String> mutator = HFactory.createMutator(ksp, stringSerializer);

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

	/**
	 * Puts multiple columns into the database asynchronously.
	 * Column name-value pairs are supplied by the key-value pairs in the map parameter.
	 * In the case where isSuperColumn is true, the columns are inserted into a supercolumn with the given
	 * name (colName) under the given key. Where isSuperColumn is false, the colName parameter is ignored and the columns are 
	 * inserted into the column family under the given key as normal.
	 * @param key The key to put the columns under
	 * @param cfName The column family to put into
	 * @param colName The name of the supercolumn to insert into
	 * @param map a set of column name / value pairs to insert
	 * @param isSuperColumn if the colName parameter should be used to put these values into a supercolumn
	 * @throws IOException if any error occours while attempting to perform this action
	 */
	public void batchPut(String key, String cfName, String colName, Map<byte[], byte[]> map, boolean isSuperColumn) throws IOException {
		LOGGER.trace("Hitting batchPut");
		try {
			Mutator<String> mutator = HFactory.createMutator(ksp, stringSerializer);

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
			mutator.execute();
		} catch (Exception e) {
			throw new IOException(e);
		} 
	}

	/**
	 * Gets the bytes stored at the given key / column path.
	 * @param key the key to look under
	 * @param column the column path to look under e.g. ColumnFamily:SuperColumn:SubColumn or ColumnFamily:Column
	 * @return the bytes stored at this location
	 * @throws IOException if any error occours while attempting to perform this action. (NotFoundException will be the cause of this exception if the columnpath/key dosen't exist)
	 */
	public byte[] get(String key, String column) throws IOException {
		LOGGER.trace("Hitting Get: Key:" + key + "  ColumnPath:"  + column);
		try {
			ColumnPath columnPath = extractColumnPath(column);
			if (columnPath.isSetSuper_column() && columnPath.isSetColumn()){
				SubColumnQuery<String, String, String, byte[]> query = HFactory.createSubColumnQuery(ksp, stringSerializer, stringSerializer, stringSerializer, byteSerializer);
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
				ColumnQuery<String, String, byte[]> query = HFactory.createColumnQuery(ksp, stringSerializer, stringSerializer, byteSerializer);
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

	/**
	 * Deletes all columns under the given key in both the file and folder column families.
	 * @param key the key to delete
	 * @throws IOException if any error occours while attempting to perform this action.
	 */
	public void delete(String key) throws IOException {
		try {
			Mutator<String> mutator = HFactory.createMutator(ksp, stringSerializer);
			mutator.addDeletion(key, FSConstants.DefaultFileCF);
			mutator.addDeletion(key, FSConstants.DefaultFolderCF);
			mutator.execute();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * Deletes the supercolumn in the given column family & key location.
	 * @param key the key to look under for the supercolumn
	 * @param columnFamily the column family
	 * @param superColumn the name of the supercolumn to delete
	 * @throws IOException if any error occours while attempting to perform this action
	 */
	public void delete(String key, String columnFamily, String superColumn) throws IOException {
		try {
			Mutator<String> mutator = HFactory.createMutator(ksp, stringSerializer);
			mutator.addSuperDelete(key, columnFamily, superColumn, stringSerializer);
			mutator.execute();
		} catch (Exception e) {
			throw new IOException(e);
		} 
	}

	/**
	 * Queries whether there are any files or folders with the given name
	 * @param key the file path e.g. /myFolder/example.txt or /myFolder/example.txt_$1 etc..
	 * @return whether somthing exists at this location
	 * @throws IOException if any error occours while attempting to perform this action
	 */
	public boolean exist(String key) throws IOException {
		try {
			//TODO: not sure this method works as i think, needs more testing!
			SubColumnQuery<String, String, String, byte[]> query = HFactory.createSubColumnQuery(ksp, stringSerializer, stringSerializer, stringSerializer, byteSerializer);
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
		} catch (Exception e) {
			throw new IOException(e);
		} 

		try {
			ColumnQuery<String, String, byte[]> query = HFactory.createColumnQuery(ksp, stringSerializer, stringSerializer, byteSerializer);
			query.setColumnFamily(FSConstants.DefaultFileCF);
			query.setName(FSConstants.ContentAttr + "_0");
			query.setKey(key);
			QueryResult<HColumn<String, byte[]>> result = query.execute();
			if(result == null || result.get() == null ) {
				throw new NotFoundException();
			}
			return true;
		} catch (NotFoundException e) {
			// do nothing, just return false
		} catch (Exception e) {
			throw new IOException(e);
		}
		return false;
	}

	/**
	 * Lists Path objects containing data about the specified folder or file.
	 * Each file or folder found in the given location (key) is returned as a Path object, the path object
	 * will also contain all the metadata associated with that object e.g. compressed size.
	 * If the folder column family is queried, then the immediate contents (not recursive) of the specified
	 * folder is returned, then if the includeFolderFlag parameter is set to true, it will also return a
	 * path for the folder flag column (default is '$_Folder_$') which contains metadata for that folder.
	 * If the file column family is queried then at most one Path object will be returned
	 * @param key the path and name of this file or folder
	 * @param columnFamily the column family to look under (only 'Folder' or 'File' currently supported)
	 * @param includeFolderFlag whether folder metadata should be included
	 * @return a list of paths containing metadata about the contents of this file or folder
	 * @throws IOException if any error occours while attempting to perform this action
	 */
	public List<Path> list(String key, String columnFamily, boolean includeFolderFlag) throws IOException {
		try {
			List<Path> children = new ArrayList<Path>();

			if (columnFamily.equals(FSConstants.DefaultFolderCF)) {
				SuperSliceQuery<String, String, String, byte[]> query = HFactory.createSuperSliceQuery(ksp, stringSerializer, stringSerializer, stringSerializer, byteSerializer);
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
				SliceQuery<String, String, byte[]> query = HFactory.createSliceQuery(ksp, stringSerializer, stringSerializer, byteSerializer);
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

	/**
	 * Returns a Path object containing the metadata about a file in the given location.
	 * Queries the FileCF for metadata about the given file. method: list provides the same functionality
	 * when used for only the FileColumn family
	 * @param file the path and name of the file to query
	 * @return path containing the requested metadata
	 * @throws IOException if any error occours while attempting to perform this action
	 */
	public Path listFile(String file) throws IOException {
		try {
			SliceQuery<String, String, byte[]> query = HFactory.createSliceQuery(ksp, stringSerializer, stringSerializer, byteSerializer);
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
