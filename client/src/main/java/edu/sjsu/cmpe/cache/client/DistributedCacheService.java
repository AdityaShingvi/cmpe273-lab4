package edu.sjsu.cmpe.cache.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class DistributedCacheService implements CacheServiceInterface {
	
	CRDTClient client_crdt;
	ConcurrentHashMap<String, String> map_status = new ConcurrentHashMap<String, String>();
	private final String url_cache;
	
	public DistributedCacheService(String serverUrl) {
		this.url_cache = serverUrl;
	}

	public DistributedCacheService(String serverUrl, ConcurrentHashMap<String, String> map_status) {
		this.url_cache = serverUrl;
		this.map_status = map_status;
	}

	public DistributedCacheService(String serverUrl, CRDTClient client_crdt) {
		this.url_cache = serverUrl;
		this.client_crdt = client_crdt;
	}

	public String getCacheServerURL(){
		return this.url_cache;
	}
	
	@Override
	public void get(long key) {
		Future<HttpResponse<JsonNode>> future = Unirest.get(this.url_cache + "/cache/{key}")
				.header("accept", "application/json")
				.routeParam("key", Long.toString(key))
				.asJsonAsync(new Callback<JsonNode>() {

					public void failed(UnirestException e) {
						System.out.println("The get request has failed");
						client_crdt.getStatus.put(url_cache, "fail");
					}

					public void completed(HttpResponse<JsonNode> response) {
						if(response.getCode() != 200) {
							client_crdt.getStatus.put(url_cache, "a");
						} else {
							String value = response.getBody().getObject().getString("value");
							System.out.println("Get value from server: "+url_cache+": "+value);
							client_crdt.getStatus.put(url_cache, value);
						}
					}

					public void cancelled() {
						System.out.println("The get request has been cancelled");
						client_crdt.getStatus.put(url_cache, "fail");
					}

				});
	}

	@Override
	public void put(long key, String value) {
		System.out.println("Sending key, value in put:"+key+", "+value);
		Future<HttpResponse<JsonNode>> future = Unirest.put(this.url_cache + "/cache/{key}/{value}")
				.header("accept", "application/json")
				.routeParam("key", Long.toString(key))
				.routeParam("value", value)
				.asJsonAsync(new Callback<JsonNode>() {

					public void failed(UnirestException e) {
						System.out.println("The put request has failed");
						client_crdt.putStatus.put(url_cache, "fail");
					}

					public void completed(HttpResponse<JsonNode> response) {
						if (response == null || response.getCode() != 200) {
							System.out.println("Failed to add to the cache.");
							client_crdt.putStatus.put(url_cache, "fail");
						} else {
							System.out.println("The put request is successfull");
							client_crdt.putStatus.put(url_cache, "pass");
						}
					}

					public void cancelled() {
						System.out.println("The put request has been cancelled");
						client_crdt.putStatus.put(url_cache, "fail");
					}

				});
	}

	@Override
	public boolean delete(long key) {
		HttpResponse<JsonNode> response = null;
		try {
			response = Unirest.delete(this.url_cache + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key)).asJson();
		} catch (UnirestException e) {
			System.err.println(e);
		}

		if(response ==null || response.getCode() != 204) {
			System.out.println("Failed to perform delete operation.");
			return false;
		} else{
			System.out.println("Successfully performed delete operation.");
			return true;
		}
	}
}
