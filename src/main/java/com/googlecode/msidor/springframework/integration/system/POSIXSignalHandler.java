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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;

/**
 * Handles and stores POSIX signals                                            
 *  
 *
 * @author Maciej SIDOR (maciejsidor@gmail.com)
 * @since 2015
 */
@SuppressWarnings("restriction")
public class POSIXSignalHandler implements sun.misc.SignalHandler
{
	
	/**
	 * logger
	 */
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * Queue with all captured signals 
	 */
	private final BlockingQueue<String> signalsReceived = new LinkedBlockingQueue<String>();
	
	/**
	 * Default constructor
	 * @param signalsList list of all signals to listen for
	 */
	public POSIXSignalHandler(List<String> signalsList)
	{
				
		if(signalsList!=null)
		{
			for (String signalName : signalsList) 
			{
			    //add signal handling
			    try
			    {
			        sun.misc.Signal.handle(new sun.misc.Signal(signalName), this);
			        log.info("Declaring as handler for the signal: "+signalName);
			    }
			    catch( IllegalArgumentException x )
			    {
			        // Most likely this is a signal that's not supported on this
			        // platform or with the JVM as it is currently configured
			        log.warn("Signal "+signalName+" is not supported by platform" );
			    }
			    catch( Throwable x )
			    {
			        // We may have a serious problem, including missing classes
			        // or changed APIs
			         log.warn("Signal "+signalName+" is not supported by system");
			    }
			}
		}
	}
	
	/**
	 * Receive signal
	 * @param arg0 - signal to handle
	 */
	public void handle(Signal arg0) 
	{
	    log.debug("Receiving Signal: "+arg0.getName());
	    
	    signalsReceived.add(arg0.getName());
	    
	}
	
	/**
	 * Pool next register signal from queue
	 * @return register signal from queue
	 */
	public String getNextSignal()
	{
		return signalsReceived.poll();
	}

}