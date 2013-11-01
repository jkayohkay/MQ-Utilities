package com.cgi.t360.util;

import java.io.*;

public class XMLFileFilter implements FilenameFilter {
	String ext;

	public XMLFileFilter(String ext) {
		this.ext = "." + ext;
	}

	public boolean accept(File dir, String name) {
		return name.toLowerCase().endsWith(ext.toLowerCase());
	}
}