package com.huawei.storage;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import cn.hutool.core.util.ByteUtil;
import com.google.common.primitives.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * 提供方法存储数据 返回数据的offset
 */
public class DataManager {

    // 持有一个文件
    FileChannel channel;

    public DataManager(String fileName) {
        RandomAccessFile randomAccessFile = FileUtil.createRandomAccessFile(FileUtil.file(fileName ),
            FileMode.rw);
        channel = randomAccessFile.getChannel();
    }

    public DataItem read(long offset) throws Exception {
        channel.position(offset);
        return nextData();
    }

    public long insert(byte[] data) throws Exception {
        long size = channel.size();
        byte[] concat = Bytes.concat(ByteUtil.shortToBytes((short) data.length), data);
        channel.write(ByteBuffer.wrap(concat), size);
        return size;
    }

    public void close() throws IOException {
        channel.close();
    }

    public void position(int i) throws IOException {
        channel.position(i);
    }

    public boolean isRemaining() throws IOException {
        return channel.size() == channel.position();
    }
    public DataItem nextData() throws IOException {
        if(isRemaining())return null;
        short size = getShort();
        ByteBuffer data = ByteBuffer.allocate(size);
        data.order(ByteOrder.LITTLE_ENDIAN);
        channel.read(data);
        data.rewind();
        return new DataItem(size,data);
    }



    public DataItem next(ByteBuffer buffer) throws IOException {
        if(!buffer.hasRemaining())return null;
        short size = getShort();
        ByteBuffer data = ByteBuffer.allocate(size);
        data.order(ByteOrder.LITTLE_ENDIAN);
        channel.read(data);
        data.rewind();
        return new DataItem(size,data);
    }

    public short getShort() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(2);
        channel.read(buf);
        return ByteUtil.bytesToShort(buf.array());
    }
    public HashMap<String, String> nextMap() throws IOException {
        ByteBuffer buffer = nextData().getBuffer();
        return unWarpToMap(buffer);
    }
    public HashMap<String, HashMap<String, String>> nextMapMap() throws IOException {
        ByteBuffer buffer = nextData().getBuffer();
        HashMap<String, HashMap<String, String>> map = new HashMap<>();
        while (buffer.hasRemaining()){
            DataItem key = next(buffer);
            DataItem value = next(buffer);
            map.put(new String(key.getBuffer().array()),unWarpToMap(value.getBuffer()));
        }
        return map;
    }
    public HashMap<String, LinkedList<String>> nextListMap() throws IOException {
        ByteBuffer buffer = nextData().getBuffer();
        HashMap<String, LinkedList<String>> map = new HashMap<>();
        while (buffer.hasRemaining()){
            DataItem key = next(buffer);
            DataItem value = next(buffer);
            map.put(new String(key.getBuffer().array()),unWarpToList(value.getBuffer()));
        }
        return map;
    }

    public HashMap<String, HashSet<String>> nextSetMap() throws IOException {
        ByteBuffer buffer = nextData().getBuffer();
        HashMap<String, HashSet<String>> map = new HashMap<>();
        while (buffer.hasRemaining()){
            DataItem key = next(buffer);
            DataItem value = next(buffer);
            map.put(new String(key.getBuffer().array()),unWarpToHashSet(value.getBuffer()));
        }
        return map;
    }

    private HashSet<String> unWarpToHashSet(ByteBuffer buffer) throws IOException {
        HashSet<String> set = new HashSet<String>();
        while (buffer.hasRemaining()){
            DataItem key = next(buffer);
            set.add(new String(key.getBuffer().array()));
        }
        return set;
    }
    //所有unwrap方法不带有长度

    private HashMap<String, String> unWarpToMap(ByteBuffer buffer) throws IOException {
        HashMap<String, String> map = new HashMap<>();
        while (buffer.hasRemaining()){
            DataItem key = next(buffer);
            DataItem value = next(buffer);
            map.put(new String(key.getBuffer().array()),new String(value.getBuffer().array()));
        }
        return map;
    }

    private LinkedList<String> unWarpToList(ByteBuffer buffer) throws IOException {
        LinkedList<String> linkedList = new LinkedList<>();
        while (buffer.hasRemaining()){
            DataItem key = next(buffer);
            linkedList.add(new String(key.getBuffer().array()));
        }
        return linkedList;
    }



}
