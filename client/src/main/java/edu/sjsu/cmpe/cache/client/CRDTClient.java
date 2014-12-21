package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class CRDTClient {
	
	public ConcurrentHashMap<String, String> list_put = new ConcurrentHashMap<String, String>();
	public ConcurrentHashMap<String, String> list_get = new ConcurrentHashMap<String, String>();
	private ArrayList<DistributedCacheService> list_servers = new ArrayList<DistributedCacheService>();
	
	public void put(long key, String value) {
		for(DistributedCacheService ser: list_servers) {
			ser.put(key, value);
		}
		
		while(true) {
        	if(list_put.size() < 3) {
        		try {
        			System.out.println("Waiting for all put request to get processed...");
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	} else{
        		int fail = 0;
        		int pass = 0;
        		for(DistributedCacheService ser: list_servers) {
        			System.out.println("put status for : "+ser.getCacheServerURL()+": "+list_put.get(ser.getCacheServerURL()));
        			if(list_put.get(ser.getCacheServerURL()).equalsIgnoreCase("fail")) 
            			fail++;
            		else
            			pass++;
        		}
        		
        		if(fail > 1) {
        			System.out.println("Rolling back put operations on all servers");
        			for(DistributedCacheService ser: list_servers) {
        				ser.delete(key);
        			}
        		} else {
        			System.out.println("Successfully updated servers");
        		}
        		list_put.clear();
        		break;
        	}
        }
	}
	
	public void addServer(String serverURL) {
		list_servers.add(new DistributedCacheService(serverURL,this));
	}
	
		
	public String get(long key){
		for(DistributedCacheService ser: list_servers) {
			ser.get(key);
		}
		
		while(true) {
        	if(list_get.size() < 3) {
        		try {
        			System.out.println("Waiting for all get request to get processed...");
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	} else{
        		HashMap<String, List<String>> getResults = new HashMap<String, List<String>>();
        		for(DistributedCacheService ser: list_servers) {
        			if(list_get.get(ser.getCacheServerURL()).equalsIgnoreCase("fail")) 
            			System.out.println("Cannot get value from server: "+ser.getCacheServerURL());
            		else {
            			if(getResults.containsKey(list_get.get(ser.getCacheServerURL()))) {
            				getResults.get(list_get.get(ser.getCacheServerURL())).add(ser.getCacheServerURL());
            			} else {
            				List<String> temp = new ArrayList<String>();
            				temp.add(ser.getCacheServerURL());
            				getResults.put(list_get.get(ser.getCacheServerURL()),temp);
            			}
            		}
        		}
        		
        		if(getResults.size() != 1) {
        			System.out.println("Values not consistent on each server");
        			Iterator<Entry<String, List<String>>> it = getResults.entrySet().iterator();
        			int majority = 0;
        			String finalValue = null;
        			ArrayList <String> updateServer = new ArrayList<String>();
        		    while (it.hasNext()) {
        		        Map.Entry<String, List<String>> pairs = (Map.Entry<String, List<String>>)it.next();
        		        if(pairs.getValue().size() > majority) {
        		        	majority = pairs.getValue().size();
        		        	finalValue = pairs.getKey();
        		        } else {
        		        	for (String s: pairs.getValue()){
        		        		updateServer.add(s);
        		        	}
        		        }
        		    }
        		    
        			System.out.println("Updating values to make the servers consistent.");
        			for(String s: updateServer){
        				for(DistributedCacheService ser: list_servers) {
            				if(ser.getCacheServerURL().equalsIgnoreCase(s)){
            					System.out.println("Correcting value for server: "+ser.getCacheServerURL()+" as: "+finalValue);
            					ser.put(key, finalValue);
            				}
            			}
        			}
        			list_get.clear();
        			return finalValue;
        		} else {
        			System.out.println("Successfully performed get function all servers");
        			list_get.clear();
        			return getResults.keySet().toArray()[0].toString();
        		}
        	}
        }
		
	}

}
