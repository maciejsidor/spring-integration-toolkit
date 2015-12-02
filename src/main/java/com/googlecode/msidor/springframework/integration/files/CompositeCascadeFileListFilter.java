/*
Copyright 2015 Maciej SIDOR [maciejsidor@gmail.com]

The source code is licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
    
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.	
 */
package com.googlecode.msidor.springframework.integration.files;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.file.filters.FileListFilter;
import org.springframework.util.Assert;


/**
 * Simple {@link FileListFilter} that predicates its matches against <b>all</b> of the
 * configured {@link FileListFilter} in cascade way.
 * 
 * This is the customized version of org.springframework.integration.file.filters.CompositeFileListFilter.
 * The difference between this implementation is in filterFiles method.
 * The original implementation always matches all files against all defined filters.
 * It means that even if a file was filtered out by one of the defined filters it still going to be matched against all other filters.
 * This eliminates usage of AcceptOnceFileListFilter and LastModifiedFileListFilter in the same filters composition.     
 *
 * @param <F> The type that will be filtered.
 * @author Maciej SIDOR
 * @since 2015
 */
public class CompositeCascadeFileListFilter<F> implements FileListFilter<F>, Closeable 
{

	/**
	 * filters to set
	 */
	private final Set<FileListFilter<F>> fileFilters;


	/**
	 * Default constructor
	 */
	public CompositeCascadeFileListFilter() 
	{
		this.fileFilters = new LinkedHashSet<FileListFilter<F>>();
	}

	/**
	 * Constructor with file filters as parameter
	 * @param fileFilters filters to set
	 */
	public CompositeCascadeFileListFilter(Collection<? extends FileListFilter<F>> fileFilters) 
	{
		this.fileFilters = new LinkedHashSet<FileListFilter<F>>(fileFilters);
	}


	/**
	 * Close all closeable filters 
	 */
	@Override
	public void close() throws IOException 
	{
		for (FileListFilter<F> filter : this.fileFilters) 
		{
			if (filter instanceof Closeable) 
			{
				((Closeable) filter).close();
			}
		}
	}

	/**
	 * @param filter one filter to add
	 * @return this CompositeFileFilter instance with the added filters
	 * @see #addFilters(Collection)
	 */	
	public CompositeCascadeFileListFilter<F> addFilter(FileListFilter<F> filter) 
	{
		return this.addFilters(Collections.singletonList(filter));
	}

	/**
	 * @param filters one or more new filters to add
	 * @return this CompositeFileFilter instance with the added filters
	 * @see #addFilters(Collection)
	 */
	public CompositeCascadeFileListFilter<F> addFilters(FileListFilter<F>... filters) 
	{
		return addFilters(Arrays.asList(filters));
	}

	/**
	 * Not thread safe. Only a single thread may add filters at a time.
	 * <p>
	 * Add the new filters to this CompositeFileListFilter while maintaining the existing filters.
	 *
	 * @param filtersToAdd a list of filters to add
	 * @return this CompositeFileListFilter instance with the added filters
	 */
	public CompositeCascadeFileListFilter<F> addFilters(Collection<? extends FileListFilter<F>> filtersToAdd) 
	{
		for (FileListFilter<? extends F> elf : filtersToAdd) 
		{
			if (elf instanceof InitializingBean) 
			{
				try 
				{
					((InitializingBean) elf).afterPropertiesSet();
				}
				catch (Exception e) 
				{
					throw new RuntimeException(e);
				}
			}
		}
		this.fileFilters.addAll(filtersToAdd);
		return this;
	}


	/**
	 * Contrary to the original implementation, this method matches files against filters based on results of previous match.
	 * @see #fileFilters
	 */
	@Override
	public List<F> filterFiles(F[] files) 
	{		
		Assert.notNull(files, "'files' should not be null");

		List<F> results = new ArrayList<F>(Arrays.asList(files));
		for (FileListFilter<F> fileFilter : this.fileFilters) 
		{					
			if(results.size()>0)
			{
				F[] filesToFilter = toArray(results);
				
				List<F> currentResults = fileFilter.filterFiles(filesToFilter);
				results.retainAll(currentResults);
			}
		}
		return results;
	}
	
	
	/**
	 * Creates generic array based on list 
	 * 
	 * @param list of objects to be transformed to array
	 * @return array of objects of the same type as the input list
	 */
	public static <F> F[] toArray(List<F> list) 
	{
	    @SuppressWarnings("unchecked")
		F[] toR = (F[]) java.lang.reflect.Array.newInstance(list.get(0).getClass(), list.size());
	    
	    for (int i = 0; i < list.size(); i++) 
	    {
	        toR[i] = list.get(i);
	    }
	    return toR;
	}

}
