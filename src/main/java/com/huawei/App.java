package com.huawei;

import com.huawei.redis.Get;
import com.huawei.redis.RedisServer;

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
        redisServer.persistRDB();
        //
        redisServer.close();
        redisServer.start();
        System.out.println(redisServer.keys());
        System.out.println(redisServer.get("4"));
        System.out.println( "Hello World!" );
    }
}
