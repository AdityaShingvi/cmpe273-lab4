package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        
        CRDTClient client = new CRDTClient();
        client.addServer("http://localhost:3000");
        client.addServer("http://localhost:3001");
        client.addServer("http://localhost:3002");
        client.put(1, "a");
        Thread.sleep(30*1000);
        client.put(1, "b");
        Thread.sleep(30*1000);
        client.put(1, "a");
        Thread.sleep(30*1000);*/
        client.put(1, "b");
        Thread.sleep(30*1000);
       	System.out.println("Value in all server: "+client.get(1));
        System.out.println("Existing Cache Client...");
    }

}
