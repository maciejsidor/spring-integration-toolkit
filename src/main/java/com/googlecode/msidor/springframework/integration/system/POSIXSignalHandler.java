package com.googlecode.msidor.springframework.integration.system;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;



import org.apache.log4j.Logger;

import sun.misc.Signal;

@SuppressWarnings("restriction")
public class POSIXSignalHandler implements sun.misc.SignalHandler
{
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final BlockingQueue<String> signalsReceived = new LinkedBlockingQueue<String>();
	
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
	
	public void handle(Signal arg0) 
	{
	    log.debug("Receiving Signal: "+arg0.getName());
	    
	    signalsReceived.add(arg0.getName());
	    
	}
	
	public String getNextSignal()
	{
		return signalsReceived.poll();
	}

}