package com.huawei.redis;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import com.google.common.primitives.Bytes;
import com.huawei.storage.DataItem;
import com.huawei.storage.DataManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class RedisServer {
    // 持有一个文件
    DataManager dataManager;

    public RedisServer() {

    }

    HashMap<String, String> map = new HashMap<>();

    public Object exec(Get get) {
        return map.get(get.getKey());
    }

    public void persistRDB() throws Exception {
        // 支持RDB方式存储
        //怎么把内存tobyte
        // 每种结构的存储形式不一样  定义hash存储格式  [size][keysize][key][valuesize][value]
        byte[] raw = new byte[0];
        for (Map.Entry<String, String> entry : map.entrySet()) {
            raw = Bytes.concat(raw, warp(entry.getKey()), warp(entry.getValue()));
        }
        dataManager.insert(raw);
    }

    private byte[] warp(String key) {
        return new byte[0];
    }

    public void set(String s, String s1) {
        map.put(s,s1);
    }

    public void close() throws IOException {
        dataManager.close();
    }

    public void start() throws IOException {
        File file = FileUtil.file(".RDB");
        dataManager = new DataManager(".RDB");
        if(file.length()!=0){
            System.out.println("开始进行rdb恢复");
            DataItem next = dataManager.next();
            // 从这里恢复map
            ByteBuffer buffer = next.getBuffer();
            while (buffer.hasRemaining()){
                DataItem key = dataManager.next(buffer);
                DataItem value = dataManager.next(buffer);
                map.put(new String(key.getBuffer().array()),new String(value.getBuffer().array()));
            }
        }
    }

    public long keys() {
        return map.size();
    }

    public String get(String s) {
        return map.get(s);
    }
}
