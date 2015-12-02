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
package com.googlecode.msidor.springframework.integration.channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.channel.AbstractPollableChannel;
import org.springframework.integration.channel.QueueChannelOperations;
import org.springframework.integration.core.MessageSelector;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Sometimes in multithreading  environment it is important that a set of data should not be handled in parallel by several threads at once.
 * This custom implementation of {@link AbstractPollableChannel} allows to lock the set of messages to be processed only by one thread at the same time where the locking is based on message's header attribute.<br/> 
 * This channel implements as well the load balancing: incoming messages are dispatched into queues based on header attribute and are send to next component by round-robin load balancing.<br/>
 * To better support the multithreading (and avoid threads starvations and livelocks) this channel blocks the consuming threads if there are no eligible messages to handle.  
 * 
 * @author Maciej SIDOR (maciejsidor@gmail.com)
 * @since 2015
 */
public class ConcurentOrderedMultiQueueChannel extends AbstractPollableChannel implements QueueChannelOperations
{
	/**
	 * Logger
	 */
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * Part of round-robin load balancing implementation: this Object keeps the current queue ID so that the next item is send from another queue. 
	 */
	private Object                              				currentQueueId  			= null;
	
	/**
	 * Part of round-robin load balancing implementation: list of currently active queues IDs 
	 */
	private ArrayList<Object>                					queuesIds       			= new ArrayList<Object>();
	
	/**
	 * List of queues identified by queues IDs
	 */
    private Map<Object,ArrayList<Message<?>>>        			queues          			= new ConcurrentHashMap<Object,ArrayList<Message<?>>>();
    
    /**
     * Map that stores locks on messages. 
     * The key is a message header value and the the value is the timestamp that indicates when the message was locked.
     * A thread that selects a message updates this map in order to prevent another thread from selecting a message with the same header value.
     * The thread that selected the message must release the lock after processing in order to allow other threads to process the messages with the same header value.
     * If it doesn't happen, other threads may ignore the lock if the lock is older that the value of timeoutForExecutionLock property.
     */
    private Map<Object, Long >       							executionsLocks 			= new ConcurrentHashMap<Object, Long>();    
    
    /**
     * Defines the timeout after which the execution lock may be ignored
     */
    private long 												timeoutForExecutionLock 	= 0;
    
    /**
     * max capacity of messages in all queues together
     */
    private int													totalCapacity				= 100;
    
    /**
     * Name of message header parameter that will be used to identify the value for message locking
     */
    private String 												executionIdHeaderParameter 	= null;
    
    /**
     * Name of message header parameter that will be used to identify the queue for load balancing
     */
    private String 												queueIdHeaderParameter 		= null;
    
    							    
    /** Lock for data accessing */
    private final ReentrantLock objectLock = new ReentrantLock();

    /** Wait queue for waiting takes */
    private final Condition newMessagesToCheck = objectLock.newCondition();    

    /** Wait queue for waiting puts */
    private final Condition notFull = objectLock.newCondition();    
    
    /** Current total number of messages */
    private final AtomicInteger count = new AtomicInteger(0);    

    /**
     * Default constructor
     */
	public ConcurentOrderedMultiQueueChannel() 
	{
		
	}
        
	/**
	 * Constructor that allows to set the default max total capacity
	 * 
	 * @param queuesCapacity max total capacity
	 */
	public ConcurentOrderedMultiQueueChannel(int queuesCapacity) 
	{
		this.totalCapacity = queuesCapacity;
	}
	

	/**
	 * Handles the demands for messages
	 * 
	 * @param timeout [ms] - time for which the thread will stay blocked if there are no eligible messages to process.
	 * @return message to process
	 */
    @Override
    protected Message<?> doReceive(long timeout) 
    {
    	log.trace("Asking for message to process");
    	
    	//the message to be returned
    	Message<?> messageToReturn = null;
    	    	
    	int c = -1;
    	long nanos = TimeUnit.MILLISECONDS.toNanos(timeout);
    	
        final ReentrantLock lock = this.objectLock;
        final AtomicInteger count = this.count;
        
        try 
        {
        		//take the lock
        		lock.lockInterruptibly();
        	
                while (messageToReturn == null) 
                { 
                	  //try to get eligible message
                	  messageToReturn = findEligibleMessage(); 
                	  
                	  //if message was found
                      if(messageToReturn!=null)
                      {                
                    	//decrement the global messages counter
      	                c = count.getAndDecrement();
      	                
      	                //if there are some messages wake up the next consumer thread to check if there is new eligible message
      	                if (c > 0)
      	                    newMessagesToCheck.signal();
                      }
                      else
                      {
                    	  //go to sleep waiting for new eligible message
                    	  
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
        	//if message was found, wake the producer thread to notify it that there is a free space to insert new message 
            if(messageToReturn!=null)
            	notFull.signal();        	
        	
            lock.unlock();
        }
        

        
        log.trace("Rreceiving message "+messageToReturn);
        
        return messageToReturn;
    }    
    
    /**
     * Try to find the eligible message.
     * Eligible means that message should not be locked by other thread or the lock has expired.
     * Message selection is made by round-robin algorithm.
     * This message should be called only when caller has an exclusive lock on object 
     *     
     * @return eligible message or null if no eligible message was found and all queues have been scanned
     */
	private Message<?> findEligibleMessage() 
	{						
		    	
        Message<?> messageToReturn = null;

        //keep the initial queue ID in memory to know when all queues were scanned and to prevent the infinite looping
        Object initialQueueId = currentQueueId;
        boolean firstIteration = true;
        
        /* The end search conditions:
         * - thread was interupted
         * - eligible message was found 
         * - all queues have been scanned
         */
        
        while(!Thread.interrupted() && messageToReturn==null && initialQueueId!=null && firstIteration == initialQueueId.equals(currentQueueId))
        {
        	
        	firstIteration = false;
                	
        	//get current queue then find the next queue id on list and set it as the current queue id 
	        List<Message<?>> messages = getCurrentQueue();

	        if(messages!=null)
	        {
	            for (Message<?> message : messages)
	            {
	            	//get the execution ID for message
	            	Object executionId = getExecutionIdFromMessage(message); 
	            		            	
	                if(executionId !=null)
	                {
	                	//check if execution ID is already locked
	                    if(executionsLocks.containsKey(executionId) )
	                    {
	                    	
	                    	//check if execution ID hasn't expired
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
	                    	//lock the execution ID 
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
	        	//remove message from queue
	            messages.remove(messageToReturn);                    		            
	        }		        				        
        }
                			
		return  messageToReturn;
	}    
    
    /**
     * Returns current queue then find the next queue id on list and set it as the current queue id 
     * @return current queue based on the current queue id field 
     */
    private List<Message<?>> getCurrentQueue()
    {       
    	//check if there are any queues
    	if(queuesIds!=null && queuesIds.size()>0)
    	{	        
    		//get queue based on current queue id value
	        List<Message<?>> currentQueue = queues.get(currentQueueId);
	        
	        //find index of the current queue id on queues list
	        int index = queuesIds.indexOf(currentQueueId);
	        
	        //if current queue id doesn't exists anymore
	        if(index==-1)
	        {
	            index=0;
	        }	        
	        else if(index+1>=queuesIds.size()) //when current queue id is last on list
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
    
    /**
     * Lock the execution ID
     * @param message to lock
     */
    private void lockExecutionId(Message<?> message) 
    {            
    	Object executionId = getExecutionIdFromMessage(message);
    	
        if(executionId!=null)
        {               
            executionsLocks.put(executionId,System.currentTimeMillis());                            
        }          
    }            
    
    /**
     * Unlock the execution ID.
     * This method lock the object to perform manipulation on locks collection and to notify dormant consumer threads that there are potential messages to check.
     * 
     * @param message to unlock
     */
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
    
	

	/**
	 * Adds message to the queue.
	 * This method blocks if there is no space.
	 * @param message to be added
	 * @param timeout after which the method awakes from waiting for free space (0 for no timeout)
	 * @return true if message was successfully added to queue
	 */
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
        	//lock the object exclusively
        	lock.lockInterruptibly();
        	
            while (count.get() == totalCapacity) 
            {
            	//if timeout was set and has elapsed
                if (nanos <= 0 && timeout>0) 
                    return false;            	
               
                //wait for notification when any message has been handled
                if(timeout>0)
                {                	 
                	nanos = notFull.awaitNanos(nanos);                	
                }
                else
                {
                	notFull.await();
                }
            }
            
            //add message to the queue
            addMessage(message);
            
            
            c = count.getAndIncrement();
           
            //if there is still some space notify any other potentially dormant producer thread
            if (c + 1 < totalCapacity)
               notFull.signal();
            
        } 
        catch (InterruptedException e) 
        {
        	log.trace("Lock interrupted by other thread");
		} 
        finally 
        {
        	//notify potentially dormant consumer thread that there is a message to handle 
        	newMessagesToCheck.signal();
        	lock.unlock();
        }
        
        
        
        return true;
    }        	
	
	/**
	 * Add message to queue based on message's header attribute.
	 * If queue doesn't exists yet it will be created.
	 * @param message to add
	 */
	private void addMessage(Message<?> message) 
	{	
	
		//get queue ID from message's header attribute
        Object queueId = getQueueIdFromMessage(message);                
        
        if(queueId==null)
            queueId="Default";
                
        //get the queue (create it if necessary)
        ArrayList<Message<?>> queue = null;
        if(queues.containsKey(queueId))
        {
            queue = queues.get(queueId);
        }
        else
        {
        	//create queue
            queue = new ArrayList<Message<?>>();
            queues.put(queueId,queue);
            queuesIds.add(queueId);
        }
        
        //set current queue id
        if(currentQueueId==null)
        {
        	currentQueueId=queueId;
        }

        //add message to queue
		queue.add(message);
	}
	
	/**
	 * Not supported in this implementation
	 */
	@Override
	public List<Message<?>> clear() 
	{
		log.warn("Clear is not yet supported");	
		
		return null;
	}

	/**
	 * Not supported in this implementation
	 */	
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

	/**
	 * @return count of elements in all queues
	 */
	@Override
	public int getQueueSize() 
	{
		return count.get();
	}

	/**
	 * @return channel remaining capacity
	 */
	@Override
	public int getRemainingCapacity() 
	{
		return totalCapacity - count.get();
	}

	/**
	 * Get the queue ID from message based on its header value
	 * @param message to get the queue id from
	 * @return queue id based on message's header value
	 */
	private Object getQueueIdFromMessage(Message<?> message)
	{
		if(getQueueIdHeaderParameter()!=null && message.getHeaders().containsKey(getQueueIdHeaderParameter()))
		{
			return message.getHeaders().get(getQueueIdHeaderParameter());
		}
				
		return null;
	}		

	/**
	 * @return message's header parameter name that indicates queue id
	 */
	public String getQueueIdHeaderParameter() 
	{
		return queueIdHeaderParameter;
	}


	/**
	 * Sets message's header parameter name that indicates queue id
	 * @param queueIdHeaderParameter message's header parameter name that indicates queue id
	 */
	public void setQueueIdHeaderParameter(String queueIdHeaderParameter) 
	{
		this.queueIdHeaderParameter = queueIdHeaderParameter;
	}


	/**
	 * Get the execution ID from message based on its header value
	 * @param message to get the execution id from
	 * @return execution id based on message's header value
	 */
	private Object getExecutionIdFromMessage(Message<?> message)
	{
		if(getExecutionIdHeaderParameter()!=null && message.getHeaders().containsKey(getExecutionIdHeaderParameter()))
		{
			return message.getHeaders().get(getExecutionIdHeaderParameter());
		}
				
		return null;
	}	
	
	/**
	 * @return message's header parameter name that indicates execution id
	 */	
	public String getExecutionIdHeaderParameter() 
	{
		return executionIdHeaderParameter;
	}

	/**
	 * Sets message's header parameter name that indicates execution id
	 * @param executionIdHeaderParameter message's header parameter name that indicates execution id
	 */	
	public void setExecutionIdHeaderParameter(String executionIdHeaderParameter) 
	{
		this.executionIdHeaderParameter = executionIdHeaderParameter;
	}
	
	/**
	 * Sets timeout after which the execution locks are ignored
	 * @param timeoutForExecutionLock timeout after which the execution locks are ignored
	 */
	public void setTimeoutForExecutionLock(long timeoutForExecutionLock)
	{
		this.timeoutForExecutionLock=timeoutForExecutionLock;
	}

}