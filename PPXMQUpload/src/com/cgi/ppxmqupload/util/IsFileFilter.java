package com.cgi.ppxmqupload.util;

import java.io.*;

public class IsFileFilter implements FileFilter {


	public IsFileFilter() {
	}

	public boolean accept(File dir) {
		return dir.isFile();
	}
}