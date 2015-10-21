package com.googlecode.msidor.springframework.integration.channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.springframework.integration.channel.AbstractPollableChannel;
import org.springframework.integration.channel.QueueChannelOperations;
import org.springframework.integration.core.MessageSelector;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

public class ConcurentOrderedMultiQueueChannel extends AbstractPollableChannel implements QueueChannelOperations
{
	
	private final Logger  										log					= Logger.getLogger(this.getClass());
	
	private Object                              				currentQueueId  	= null;
	private ArrayList<Object>                					queuesIds       	= new ArrayList<Object>();
    private Map<Object,ArrayList<Message<?>>>        			queues          	= new ConcurrentHashMap<Object,ArrayList<Message<?>>>();
    
    private Map<Object, Long >       							executionsLocks 	= new ConcurrentHashMap<Object, Long>();    
    
    private int													totalCapacity		= 100;
    
    private String 												executionIdHeaderParameter 	= null;
    private String 												queueIdHeaderParameter 		= null;
    
    private long timeoutForExecutionLock = 0;							
    
    
    /** Lock hobjectLockake, poll, etc */
    private final ReentrantLock objectLock = new ReentrantLock();

    /** Wait queue for waiting takes */
    private final Condition newMessagesToCheck = objectLock.newCondition();    

    /** Wait queue for waiting puts */
    private final Condition notFull = objectLock.newCondition();    
    
    /** Current number of elements */
    private final AtomicInteger count = new AtomicInteger(0);    

	public ConcurentOrderedMultiQueueChannel() 
	{
		
	}
        
	public ConcurentOrderedMultiQueueChannel(int queuesCapacity) 
	{
		this.totalCapacity = queuesCapacity;
	}
	

    @Override
    protected Message<?> doReceive(long timeout) 
    {
    	log.trace("Asking for message to process");
    	
    	Message<?> messageToReturn = null;
    	
    	int c = -1;
    	long nanos = TimeUnit.MILLISECONDS.toNanos(timeout);
    	    	
        final ReentrantLock lock = this.objectLock;
        final AtomicInteger count = this.count;
        
        try 
        {
        		lock.lockInterruptibly();
        	
                while (messageToReturn == null) 
                { 
                	  messageToReturn = findEligibleMessage(); 
                	  
                      if(messageToReturn!=null)
                      {                
      	                c = count.getAndDecrement();
      	                
      	                if (c > 0)
      	                    newMessagesToCheck.signal();
                      }
                      else
                      {
	                	  if(timeout<=0)
	                	  {
	                		  newMessagesToCheck.await();
	                	  }
	                	  else
	                	  {	                		  
	                		  nanos = newMessagesToCheck.awaitNanos(nanos);
	                		  
	    					  if (nanos <= 0) return null;                		  
	                	  }
                      }
                }
        } 
        catch(InterruptedException e)
        {
        	log.trace("Lock interrupted by other thread");
        }
        finally 
        {
            if(messageToReturn!=null)
            	notFull.signal();        	
        	
            lock.unlock();
        }
        

        
        log.trace("Rreceiving message "+messageToReturn);
        
        return messageToReturn;
    }    
    
    private List<Message<?>> getCurrentQueue()
    {       
    	if(queuesIds!=null && queuesIds.size()>0)
    	{	        
	        List<Message<?>> currentQueue = queues.get(currentQueueId);
	        
	        int index = queuesIds.indexOf(currentQueueId);
	        if(index==-1)
	        {
	            index=0;
	        }	        
	        else if(index+1>=queuesIds.size())
	        {
	            index=0;
	        }
	        else
	            index++;
	        
	        currentQueueId = queuesIds.get(index);
	        
	        
	        return currentQueue;
    	}
    	else
    		return null;
    }   
    
    
    private void lockExecutionId(Message<?> message) 
    {            
    	Object executionId = getExecutionIdFromMessage(message);
    	
        if(executionId!=null)
        {               
            executionsLocks.put(executionId,System.currentTimeMillis());                            
        }          
    }            
    
    public void unlockExecutionId(Message<?> message) 
    {            
        final ReentrantLock lock = this.objectLock;
        
        try 
        {
        	lock.lockInterruptibly();    	
    	
	    	Object executionId = getExecutionIdFromMessage(message);
	    	
	    	log.trace("Unlocking execution "+executionId);
	    	
	        if(executionId!=null)
	        {               
	            executionsLocks.remove(executionId);                            
	        }
        } 
        catch (InterruptedException e)
        {
			log.trace("Lock interrupted by other thread");
		}
        finally
        {
        	newMessagesToCheck.signal();
        	lock.unlock();        	
        }
    }          
    
	
	protected Message<?> findEligibleMessage() 
	{						
		    	
        Message<?> messageToReturn = null;

        Object initialQueueId = currentQueueId;
        boolean firstIteration = true;
        
        while(!Thread.interrupted() && messageToReturn==null && initialQueueId!=null && firstIteration == initialQueueId.equals(currentQueueId))
        {
        	
        	firstIteration = false;
                	
	        List<Message<?>> messages = getCurrentQueue();
	                                
	        if(messages!=null)
	        {
	            for (Message<?> message : messages)
	            {
	            	Object executionId = getExecutionIdFromMessage(message); 
	            	
	                if(executionId !=null)
	                {                
	                    if(executionsLocks.containsKey(executionId) )
	                    {
	                    	
	                    	if(System.currentTimeMillis()-executionsLocks.get(executionId).longValue()<timeoutForExecutionLock)
	                    	{	                    	
	                    		log.trace("Trying to pull message "+message+" but the Execution ID "+executionId+" is being held by other thread");
	                    	}
	                    	else
	                    	{
	                    		log.trace(executionId+" is being held by other thread but the lock is too old thus releasing the lock");
		                        messageToReturn = message;
	                    	}
	                    }	                    
	                    else
	                    {                   
	                        messageToReturn = message;
	                    }
	                    
	                    if(messageToReturn!=null)
	                    {
	                    	lockExecutionId(message); 
	                        break;
	                    }	                    	                   
	                }
	                else
	                {
	                    messageToReturn = message;
	                    break;
	                }
	            }
	        }
	        
	        if(messageToReturn!=null)
	        {
	            messages.remove(messageToReturn);                    		            
	        }		        				        
        }
                			
		return  messageToReturn;
	}
	
	@Override
	protected boolean doSend(Message<?> message, long timeout) 
    {
		Assert.notNull(message, "'message' must not be null");
		
		log.trace("Sending message "+message);		

        long nanos = TimeUnit.MILLISECONDS.toNanos(timeout);
        int c = -1;
        final ReentrantLock lock = this.objectLock;
        final AtomicInteger count = this.count;        
        try 
        {
        	lock.lockInterruptibly();
        	
            while (count.get() == totalCapacity) 
            {
            	
                if (nanos <= 0 && timeout>0) 
                    return false;            	
               
                if(timeout>0)
                {
                	nanos = notFull.awaitNanos(nanos);                	
                }
                else
                {
                	notFull.await();
                }
            }
            
            addMessage(message);
            
            c = count.getAndIncrement();
           
            if (c + 1 < totalCapacity)
               notFull.signal();
            
        } 
        catch (InterruptedException e) 
        {
        	log.trace("Lock interrupted by other thread");
		} 
        finally 
        {
        	newMessagesToCheck.signal();
        	lock.unlock();
        }
        
        
        
        return true;
    }        	
	
	
	protected void addMessage(Message<?> message) 
	{	
	
        Object queueId = getQueueIdFromMessage(message);                
        
        if(queueId==null)
            queueId="Default";
                
        ArrayList<Message<?>> queue = null;
        if(queues.containsKey(queueId))
        {
            queue = queues.get(queueId);
        }
        else
        {
            queue = new ArrayList<Message<?>>();
            queues.put(queueId,queue);
            queuesIds.add(queueId);
        }
        
        if(currentQueueId==null)
        {
        	currentQueueId=queueId;
        }

		queue.add(message);
	}

	@Override
	public List<Message<?>> clear() 
	{
		log.warn("Clear is not yet supported");	
		
		return null;
	}

	@Override
	public List<Message<?>> purge(MessageSelector selector) 
	{
		if (selector == null) 
		{
			return this.clear();
		}
		
		log.warn("Purge is not yet supported");	
		
		return null;
	}

	@Override
	public int getQueueSize() 
	{
		return count.get();
	}

	@Override
	public int getRemainingCapacity() 
	{
		return totalCapacity - count.get();
	}

	

	private Object getQueueIdFromMessage(Message<?> message)
	{
		if(getQueueIdHeaderParameter()!=null && message.getHeaders().containsKey(getQueueIdHeaderParameter()))
		{
			return message.getHeaders().get(getQueueIdHeaderParameter());
		}
				
		return null;
	}		

	public String getQueueIdHeaderParameter() 
	{
		return queueIdHeaderParameter;
	}


	public void setQueueIdHeaderParameter(String queueIdHeaderParameter) 
	{
		this.queueIdHeaderParameter = queueIdHeaderParameter;
	}


	private Object getExecutionIdFromMessage(Message<?> message)
	{
		if(getExecutionIdHeaderParameter()!=null && message.getHeaders().containsKey(getExecutionIdHeaderParameter()))
		{
			return message.getHeaders().get(getExecutionIdHeaderParameter());
		}
				
		return null;
	}	
	
	public String getExecutionIdHeaderParameter() 
	{
		return executionIdHeaderParameter;
	}


	public void setExecutionIdHeaderParameter(String executionIdHeaderParameter) 
	{
		this.executionIdHeaderParameter = executionIdHeaderParameter;
	}
	
	public void setTimeoutForExecutionLock(long timeoutForExecutionLock)
	{
		this.timeoutForExecutionLock=timeoutForExecutionLock;
	}

}