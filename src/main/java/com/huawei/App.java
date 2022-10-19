package com.huawei;

import com.huawei.redis.Get;
import com.huawei.redis.RedisServer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        RedisServer redisServer = new RedisServer();
        redisServer.start();
        for (int i = 0; i < 10000; i++) {
            String s = i + "";
            redisServer.set(s, s);
        }

        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("hashKey","hashVal");
        redisServer.getMapMap().put("mapkey",hashMap);

        LinkedList<String> list = new LinkedList<>();
        list.add("listVal");
        redisServer.getListMap().put("listKey",list);

        HashSet< String> hashSet = new HashSet<>();
        hashSet.add("setKey");
        redisServer.getSetMap().put("setKey",hashSet);

        redisServer.persistRDB();
        //
        redisServer.close();
        redisServer.start();
        System.out.println(redisServer.keys());
        System.out.println(redisServer.get("4"));
        System.out.println( "Hello World!" );
    }
}
