package org.apache.cassandra.contrib.fs;

import java.util.List;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.contrib.fs.util.Bytes;

/**
 * Represents a path to a file or folder in cassandraFS and also contains meta-data about that file
 * @author zhanje, Edd King
 * 
 */
public class Path {

	// This value only will be used in cli for formatting
	public static int MaxSizeLength;
	private String url;
	private String name;
	private boolean isDir;
	private long length;
	private long compressedLength;
	private String last_modification_time;
	private String owner;
	private String group;
	
	/**
	 * Make a new Path to a file without any metadata
	 * @param url the url to the file
	 */
	public Path(String url) {
		this(url, false);
	}

	/**
	 * Makes a new path to a file or directory without any metadata
	 * @param url the url to the file or folder
	 * @param isDir if this is a folder or not
	 */
	public Path(String url, boolean isDir) {
		this.url = url;
		this.name = getNameFromURL(url);
		this.isDir = isDir;
	}

	private String getNameFromURL(String url) {
		int index = url.lastIndexOf("/");
		if (index == 0 && url.length() == 1) {
			return "/";
		} else {
			return url.substring(index + 1);
		}
	}

	/**
	 * Makes a new Path given its url and metadata about that file as returned by a query to CassandraFS
	 * @param url the url to the file or folder
	 * @param attributes meta data about this file
	 */
	public Path(String url, List<HColumn<String, byte[]>> attributes) {
		this.url = url;
		this.name = getNameFromURL(url);
		for (HColumn<String, byte[]> attr : attributes) {
			String attrName = attr.getName();
			if (attrName.equals(FSConstants.TypeAttr)) {
				String value = new String(attr.getValue());
				if (value.equals("File")) {
					this.isDir = false;
				} else {
					this.isDir = true;
				}
			} else if (attrName.equals(FSConstants.LastModifyTime)) {
				this.last_modification_time = Bytes.toString(attr.getValue());
			} else if (attrName.equals(FSConstants.OwnerAttr)) {
				this.owner = Bytes.toString(attr.getValue());
			} else if (attrName.equals(FSConstants.GroupAttr)) {
				this.group = Bytes.toString(attr.getValue());
			} else if (attrName.equals(FSConstants.LengthAttr)) {
				this.length = Bytes.toLong(attr.getValue());
			}else if (attrName.equals(FSConstants.CompressedLengthAttr)) {
				this.compressedLength = Bytes.toLong(attr.getValue());
			}
		}
	}

	/**
	 * 
	 * @return if this is a directory (folder) or not
	 */
	public boolean isDir() {
		return this.isDir;
	}

	/**
	 * 
	 * @return the url path to this file
	 */
	public String getURL() {
		return this.url;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(isDir ? "d " : "- ");
		builder.append(String.format("%-8s", owner));
		builder.append(String.format("%-14s", group));
		builder.append(String.format("%- " + (MaxSizeLength + 2) + "d", length));
		builder.append(String.format("%16s", last_modification_time));
		builder.append(" " + url);
		return builder.toString();
	}

	/**
	 * 
	 * @return the name of this file or folder
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * 
	 * @return the length in bytes of this file before it was stored in the file system
	 */
	public long getLength() {
		return this.length;
	}
	
	/**
	 * 
	 * @return the length in bytes of this file once it had been compressed and stored
	 */
	public long getCompressedLength() {
		return compressedLength;
	}

}
