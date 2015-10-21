package com.googlecode.msidor.springframework.integration.files;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.springframework.integration.file.filters.FileListFilter;

public class ModifTimeFileListFilter implements FileListFilter<File> 
{

	private long minLifeTime = 0;
	
	public long getMinLifeTime() 
	{
		return minLifeTime;
	}

	public void setMinLifeTime(long minLifeTime) 
	{
		this.minLifeTime = minLifeTime;
	}

	public List<File> filterFiles(File[] arg0) 
	{		
		ArrayList<File> files = new ArrayList<File>();
		
		for (File file : arg0) 
		{
			if((System.currentTimeMillis()-file.lastModified()>=minLifeTime && minLifeTime>0) || minLifeTime<=0 )
			{
				files.add(file);
			}
		}
		
		
		return files;
	}

}
