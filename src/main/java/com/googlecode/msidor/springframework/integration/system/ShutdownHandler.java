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
package com.googlecode.msidor.springframework.integration.system;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

/**
 * Handles shutdown of spring integration components.
 * Additionally the thread pool task executors are monitored to make sure that they finished they work.
 * Optionally, this component may shutdown the entire application by calling System.exit command. 
 * 
 * @author Maciej SIDOR (maciejsidor@gmail.com)
 * @since 2015
 */
public class ShutdownHandler
{
	/**
	 * Logger
	 */
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * Channel that manages the spring integration beans. Will be used to send shutdown command to selected spring integration beans
	 */
	private MessageChannel operationChannel = null;
	
	/**
	 * List of spring integration beans IDs that will be shutdown 
	 */
	private List<String> componentsToShutDown = null;
	
	/**
	 * List of Thread Pool Task Executors to wait for before shutting down the application
	 */
	private List<ThreadPoolTaskExecutor> executorsToWatch = null;
	
	/**
	 * Timeout after which the application shutdowns even if there are still tasks being executed by the Thread Pool Task Executors
	 */
	private long timeout = 0;
	
	/**
	 * Variable that indicates if every condition is satisfied to shutdown the application
	 */
	private boolean okToShutdown = false;
	
	/**
	 * Indicates if component should shutdown entire application
	 */
	private boolean shutodwnApplication = false;
	
	/**
	 * System exit code if application is shutdown
	 */
	private int systemExitCode = 0;
	
	
	
	/**
	 * Handles application shutdown
	 */
	public void shutdownGently()
	{			
		
		long startTimestamp = System.currentTimeMillis();
		
		log.info("Shuting down the application...");

		
		if(componentsToShutDown!=null)
		{			
			
			Assert.notNull(operationChannel,"Operation channel must be set");			
			
			for (String component : componentsToShutDown) 
			{
				log.info("Sending shutdown command to component: ".concat(component));
				
            	Message<String> operation = MessageBuilder.withPayload("@"+component+".stop()").build();
            	operationChannel.send(operation);

			}
		}
		
		if(executorsToWatch!=null)
		{						
			log.info("Checking if all executor threads have been accomplished...");
				
			boolean allDone = true;
			do
			{
				allDone = true;
				for (ThreadPoolTaskExecutor executor : executorsToWatch) 
				{
					allDone = allDone && executor.getActiveCount()==0;
				}
			}
			while(!allDone && !Thread.interrupted() && ((timeout>0 && System.currentTimeMillis()-startTimestamp<timeout) || timeout<=0)   );
			
			if(allDone)
				log.info("No more active threads");
			else
				log.warn("Some threads are still working");
		}
		
		
		okToShutdown = true;
		
		if(shutodwnApplication)
		{
			System.exit(systemExitCode);
		}
					
		
	}

	/**
	 * Return list of spring integration beans IDs that will be shutdown 
	 * @return List of spring integration beans IDs that will be shutdown 
	 */
	public List<String> getComponentsToShutDown() 
	{
		return componentsToShutDown;
	}

	/**
	 * Sets list of spring integration beans IDs that will be shutdown 
	 * @param componentsToShutDown List of spring integration beans IDs that will be shutdown 
	 */
	public void setComponentsToShutDown(List<String> componentsToShutDown) 
	{
		this.componentsToShutDown = componentsToShutDown;
	}


	/**
	 * Returns list of Thread Pool Task Executors to wait for before shutting down the application
	 * @return List of Thread Pool Task Executors to wait for before shutting down the application
	 */
	public List<ThreadPoolTaskExecutor> getExecutorsToWatch() 
	{
		return executorsToWatch;
	}

	/**
	 * Sets list of Thread Pool Task Executors to wait for before shutting down the application
	 * @param executorsToWatch List of Thread Pool Task Executors to wait for before shutting down the application
	 */
	public void setExecutorsToWatch(List<ThreadPoolTaskExecutor> executorsToWatch) 
	{
		this.executorsToWatch = executorsToWatch;
	}

	/**
	 * Returns channel that manages the spring integration beans. Will be used to send shutdown command to selected spring integration beans
	 * @return Channel that manages the spring integration beans. Will be used to send shutdown command to selected spring integration beans
	 */
	public MessageChannel getOperationChannel() 
	{
		return operationChannel;
	}

	/**
	 * Sets channel that manages the spring integration beans. Will be used to send shutdown command to selected spring integration beans
	 * @param Channel that manages the spring integration beans. Will be used to send shutdown command to selected spring integration beans
	 */
	public void setOperationChannel(MessageChannel operationChannel) 
	{
		this.operationChannel = operationChannel;
	}


	/**
	 * Returns timeout after which the application shutdowns even if there are still tasks being executed by the Thread Pool Task Executors
	 * @return Timeout after which the application shutdowns even if there are still tasks being executed by the Thread Pool Task Executors
	 */
	public long getTimeout() 
	{
		return timeout;
	}

	/**
	 * Sets timeout after which the application shutdowns even if there are still tasks being executed by the Thread Pool Task Executors
	 * @param Timeout after which the application shutdowns even if there are still tasks being executed by the Thread Pool Task Executors
	 */
	public void setTimeout(long timeout) 
	{
		this.timeout = timeout;
	}

	/**
	 * Returns true if every condition is satisfied to shutdown the application
	 * @return true if every condition is satisfied to shutdown the application
	 */
	public boolean isOkToShutdown() 
	{
		return okToShutdown;
	}

	/**
	 * Sets system exit code if application is shutdown
	 * @param system exit code if application is shutdown
	 */
	public void setSystemExitCode(int systemExitCode) 
	{
		this.systemExitCode = systemExitCode;
	}


	/**
	 * Sets flag that indicates if component should shutdown entire application
	 * @param true if component should shutdown entire application
	 */
	public void setShutodwnApplication(boolean shutodwnApplication) 
	{
		this.shutodwnApplication = shutodwnApplication;
	}

}
