package com.huawei.redis;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import com.google.common.primitives.Bytes;
import com.huawei.storage.DataManager;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class RedisServer {
    // 持有一个文件
    FileChannel channel;
    DataManager dataManager;

    public RedisServer() {
        dataManager = new DataManager("");
    }

    HashMap<String, String> map;

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
}
