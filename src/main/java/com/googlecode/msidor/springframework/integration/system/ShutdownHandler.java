package com.googlecode.msidor.springframework.integration.system;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

public class ShutdownHandler
{
	private final Logger log = Logger.getLogger(this.getClass());
	
	private MessageChannel operationChannel = null;
	
	private List<String> componentsToShutDown = null;
	
	private List<ThreadPoolTaskExecutor> executorsToWatch = null;
	
	private long timeout = 0;
	
	private boolean okToShutdown = false;
	
	private int systemExitCode = 0;
	
	private boolean shutodwnApplication = false;
	

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


	public List<String> getComponentsToShutDown() 
	{
		return componentsToShutDown;
	}


	public void setComponentsToShutDown(List<String> componentsToShutDown) 
	{
		this.componentsToShutDown = componentsToShutDown;
	}


	public List<ThreadPoolTaskExecutor> getExecutorsToWatch() 
	{
		return executorsToWatch;
	}


	public void setExecutorsToWatch(List<ThreadPoolTaskExecutor> executorsToWatch) 
	{
		this.executorsToWatch = executorsToWatch;
	}


	public MessageChannel getOperationChannel() 
	{
		return operationChannel;
	}


	public void setOperationChannel(MessageChannel operationChannel) 
	{
		this.operationChannel = operationChannel;
	}


	public long getTimeout() 
	{
		return timeout;
	}


	public void setTimeout(long timeout) 
	{
		this.timeout = timeout;
	}


	public boolean isOkToShutdown() 
	{
		return okToShutdown;
	}


	public void setOkToShutdown(boolean okToShutdown) 
	{
		this.okToShutdown = okToShutdown;
	}


	public int getSystemExitCode() 
	{
		return systemExitCode;
	}


	public void setSystemExitCode(int systemExitCode) 
	{
		this.systemExitCode = systemExitCode;
	}


	public boolean isShutodwnApplication() 
	{
		return shutodwnApplication;
	}


	public void setShutodwnApplication(boolean shutodwnApplication) 
	{
		this.shutodwnApplication = shutodwnApplication;
	}

}
