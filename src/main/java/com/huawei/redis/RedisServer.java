package com.huawei.redis;

import cn.hutool.core.io.FileUtil;
import com.google.common.primitives.Bytes;
import com.huawei.storage.DataManager;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

@Data
public class RedisServer {
    // 持有一个文件
    DataManager dataManager;

    public RedisServer() {

    }

    HashMap<String, String> map = new HashMap<>();
    HashMap<String, HashMap<String, String>> mapMap = new HashMap<>();
    HashMap<String, LinkedList<String>> listMap = new HashMap<>();
    HashMap<String, HashSet<String>> setMap = new HashMap<>();
    //  HashMap<String, TreeSet<String>> sortedsetMap = new HashMap<>();

    public Object exec(Get get) {
        return map.get(get.getKey());
    }

    public void persistRDB() throws Exception {
        // 支持RDB方式存储
        //怎么把内存tobyte
        // 每种结构的存储形式不一样  定义hash存储格式  [size][keysize][key][valuesize][value]
        dataManager.insert(warp(map));

        //     HashMap<String, HashMap<String, String> > mapMap 的格式  [size][keysize][key][size][keysize][key][valuesize][value]
        byte[] raw = new byte[0];
        for (Map.Entry<String, HashMap<String, String>> entry : mapMap.entrySet()) {
            raw = Bytes.concat(raw, warp(entry.getKey()), warp(entry.getValue()));
        }
        dataManager.insert(raw);

        //  HashMap<String, LinkedList<String>> listMap = new HashMap<>();
        raw = new byte[0];
        for (Map.Entry<String, LinkedList<String>> entry : listMap.entrySet()) {
            raw = Bytes.concat(raw, warp(entry.getKey()), warp(entry.getValue()));
        }
        dataManager.insert(raw);

        //  HashMap<String, HashSet<String>> setMap = new HashMap<>();
        raw = new byte[0];
        for (Map.Entry<String, HashSet<String>> entry : setMap.entrySet()) {
            raw = Bytes.concat(raw, warp(entry.getKey()), warp(entry.getValue()));
        }
        dataManager.insert(raw);
    }

    private byte[] warp(HashSet<String> value) {
        byte[] raw = new byte[0];
        for (String key : value) {
            raw = Bytes.concat(raw, warp(key));
        }
        return raw;
    }

    private byte[] warp(LinkedList<String> value) {
        byte[] raw = new byte[0];
        for (String key : value) {
            raw = Bytes.concat(raw, warp(key));
        }
        return raw;
    }

    private byte[] warp(HashMap<String, String> value) {
        byte[] raw = new byte[0];
        for (Map.Entry<String, String> entry : value.entrySet()) {
            raw = Bytes.concat(raw, warp(entry.getKey()), warp(entry.getValue()));
        }
        return raw;
    }

    private byte[] warp(String key) {
        return new byte[0];
    }

    public void set(String s, String s1) {
        map.put(s, s1);
    }

    public void close() throws IOException {
        dataManager.close();
    }

    public void start() throws IOException {
        File file = FileUtil.file(".RDB");
        dataManager = new DataManager(".RDB");
        if (file.length() != 0) {
            System.out.println("开始进行rdb恢复");
            if(dataManager.isRemaining())
            map = dataManager.nextMap();
            // 从这里恢复map
            if(dataManager.isRemaining())
            mapMap = dataManager.nextMapMap();
            if(dataManager.isRemaining())
            listMap = dataManager.nextListMap();
            if(dataManager.isRemaining())
            setMap = dataManager.nextSetMap();
        }
    }

    public long keys() {
        return map.size();
    }

    public String get(String s) {
        return map.get(s);
    }
}
