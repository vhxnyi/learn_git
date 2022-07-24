package com.big.data.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

class KpiWritable implements Writable {

    long upPackNum;
    long downPackNum;
    long upPayLoad;
    long downPayLoad;

    public KpiWritable() {
    }

    public KpiWritable(String upPackNum, String downPackNum, String upPayLoad,
            String downPayLoad) {
        this.upPackNum = Long.parseLong(upPackNum);
        this.downPackNum = Long.parseLong(downPackNum);
        this.upPayLoad = Long.parseLong(upPayLoad);
        this.downPayLoad = Long.parseLong(downPayLoad);
    }

    public void readFields(DataInput in) throws IOException {
        this.upPackNum = in.readLong();
        this.downPackNum = in.readLong();
        this.upPayLoad = in.readLong();
        this.downPayLoad = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(upPackNum);
        out.writeLong(downPackNum);
        out.writeLong(upPayLoad);
        out.writeLong(downPayLoad);
    }

    @Override
    public String toString() {
        return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;
    }
}
