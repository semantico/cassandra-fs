package org.apache.cassandra.contrib.fs;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Abstraction for a virtual file system
 *
 */
public interface IFileSystem {

	/**
	 * Creates a file with a small amount of content
	 * @param path the full path to the file and its name
	 * @param content the content of the file
	 * @throws IOException
	 */
	public abstract void createFile(String path, byte[] content)
			throws IOException;

	/**
	 * Creates a file with a stream to be read from, use this over createFile(String,byte[]) when the size
	 * of the file is too big to be held in memory or of unknown length.
	 * @param path the full path to the file and its name 
	 * @param in the content of the file
	 * @throws IOException
	 */
	public abstract void createFile(String path, InputStream in)
		throws IOException;
	
	/**
	 * Deletes a file, <b>does not</b> throw an error if the file doesnt exist
	 * @param path the full path to the file and its name 
	 * @return true if the file was deleted, false if it didnt exist
	 * @throws IOException
	 */
	public abstract boolean deleteFile(String path) throws IOException;

	/**
	 * Deletes a directory
	 * if not recursive then the delete will fail if the folder is not empty
	 * @param path the full path to the file and its name 
	 * @param recursive whether sub directories should also be recursively deleted
	 * @return true if the directory was deleted, false if it was not deleted or didnt exist
	 * @throws IOException
	 */
	public abstract boolean deleteDir(String path, boolean recursive)throws IOException;

	/**
	 * Provides an input stream in order to read a file from the file system
	 * @param path the full path to the file and its name 
	 * @return A stream to read the content of this file
	 * @throws IOException if there is a problem reading this file or it dosent exist
	 */
	public abstract InputStream readFile(String path) throws IOException;
	
	/**
	 * Makes a directory with the given path
	 * @param path the path to this directory and its name
	 * @return true if the directory was created, false otherwise
	 * @throws IOException
	 */
	public abstract boolean mkdir(String path) throws IOException;

	/**
	 * Lists the files in the given directory or a single file if it exists.
	 * The Path objects contain metadata about the found files
	 * @param path the path to the directory or file to be listed
	 * @return the list of paths containing data about the files found
	 * @throws IOException
	 */
	public abstract List<Path> list(String path) throws IOException;

	/**
	 * Lists all files in the given directory including meta information about the folder itself
	 * @param path the path to the directory
	 * @return all files and folders contained within this directory (but not contents of subfolders)
	 * @throws IOException
	 */
	public abstract List<Path> listAll(String path) throws IOException;

	/**
	 * @param path path to a directory
	 * @return true if the supplied path is an existing directory
	 * @throws IOException
	 */
	public abstract boolean existDir(String path) throws IOException;

	/**
	 * @param path path to a file
	 * @return true if the supplied path is an existing file
	 * @throws IOException
	 */
	public abstract boolean existFile(String path) throws IOException;

	/**
	 * @param path path to a file or directory
	 * @return true if the supplied path is an existing file or folder
	 * @throws IOException
	 */
	public abstract boolean exist(String path) throws IOException;

}