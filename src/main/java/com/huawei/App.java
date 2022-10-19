package com.huawei;

import com.huawei.redis.Get;
import com.huawei.redis.RedisServer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        RedisServer redisServer = new RedisServer();
        redisServer.exec(new Get());
        System.out.println( "Hello World!" );
    }
}
