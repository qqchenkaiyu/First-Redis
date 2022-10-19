package com.huawei.redis;

import java.util.HashMap;

public class RedisServer {
    HashMap<String,String> map;
    public Object exec(Get get) {
        return map.get(get.getKey());
    }
}
