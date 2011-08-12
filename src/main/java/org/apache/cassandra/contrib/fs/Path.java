package org.apache.cassandra.contrib.fs;

import java.util.List;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.contrib.fs.util.Bytes;

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
	
	public Path(String url) {
		this(url, false);
	}

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

	public boolean isDir() {
		return this.isDir;
	}

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

	public String getName() {
		return this.name;
	}

	public long getLength() {
		return this.length;
	}
	
	public long getCompressedLength() {
		return compressedLength;
	}

}
