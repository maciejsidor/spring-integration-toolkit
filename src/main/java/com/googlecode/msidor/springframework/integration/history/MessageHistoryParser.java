package com.googlecode.msidor.springframework.integration.history;

import java.util.Iterator;
import java.util.Properties;

import org.springframework.integration.history.MessageHistory;
import org.springframework.messaging.MessageHeaders;

public class MessageHistoryParser 
{
	public static String parse(MessageHeaders mh)
	{
		StringBuilder sb = new StringBuilder();
		
		MessageHistory history =  mh.get(MessageHistory.HEADER_NAME, MessageHistory.class);
		String[] 	names = new String[history.size()];
		long[] 		times = new long[history.size()];
		
		Iterator<Properties> historyIterator = history.iterator();
		int i=0;
		while(historyIterator.hasNext())
		{
			Properties 	gatewayHistory 		= historyIterator.next();
			String 		name 				= gatewayHistory.getProperty("name");
			String 		historyTimestampStr = gatewayHistory.getProperty("timestamp");
			Long 		historyTimestamp 	= Long.parseLong(historyTimestampStr);
			
			names[i]=name;
			times[i++]=historyTimestamp;
		}
		
		
		final long lastTimestamp  = mh.getTimestamp();
		for (int j = 0; j < names.length; j++) 
		{
			if(j > 0)
			{
				sb.append("; ");
			}
			
			if(j +1 < names.length)
				sb.append(names[j]).append("=").append(times[j+1]-times[j]);
			else
				sb.append(names[j]).append("=").append(lastTimestamp-times[j]);
		}
		
		if(sb.length()>0)
		{
			sb.append("; ");
		}
				
		sb.append("total=").append(lastTimestamp-times[0]);
		
		return sb.toString();
	}
}
