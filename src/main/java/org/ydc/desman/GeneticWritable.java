package org.ydc.desman;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GeneticWritable
extends BytesWritable {
    double[] data;

    public byte[] getGenetic() {
        return super.getBytes();
    }

    public void setGenetic(byte[] genetic) {
        super.set(genetic, 0, genetic.length);
    }

    public double[] getData() {
        return this.data;
    }

    public void setData(double[] data) {
        this.data = data;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.data = new double[in.readInt()];
        for(int i = 0; i < this.data.length; i++) {
            this.data[i] = in.readDouble();
        }
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.data.length);
        for(int i = 0; i < this.data.length; i++) {
            out.writeDouble(this.data[i]);
        }
        super.write(out);
    }

    @Override
    public int compareTo(BinaryComparable other) {
        int result = super.compareTo(other);

        if(result == 0 && (other instanceof GeneticWritable)) {
            GeneticWritable that = (GeneticWritable) other;
            for(int i = 0; i < this.data.length && i < that.data.length && result == 0; i++)
            {
                result = -Double.compare(this.data[i], that.data[i]);
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof GeneticWritable)) {
            return false;
        }
        GeneticWritable other = (GeneticWritable)o;
        return super.equals(other) && Arrays.equals(this.data, other.data);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Arrays.hashCode(this.data);
    }

    @Override
    public String toString() {
        return String.format("%s %s", super.toString(), Arrays.toString(this.data));
    }

    public static class Comparator
    extends WritableComparator {
        BytesWritable.Comparator bytesWritableComparator = new BytesWritable.Comparator();

        public Comparator() {
            super(GeneticWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int dl1 = readInt(b1, s1) * 8 + 4;
            int dl2 = readInt(b2, s2) * 8 + 4;

            int result = bytesWritableComparator.compare(b1, s1 + dl1, l1, b2, s2 + dl2, l2);

            for(int i = 4; i < dl1 && i < dl2 && result == 0; i+= 8) {
                double data1 = readDouble(b1, s1 + i);
                double data2 = readDouble(b2, s2 + i);

                result = -Double.compare(data1, data2);
            }

            return result;
        }
    }


    public static class Partitioner<T>
    extends org.apache.hadoop.mapreduce.Partitioner<GeneticWritable, T> {

        @Override
        public int getPartition(GeneticWritable key, T value, int numPartitions) {
            byte[] genetic = key.getGenetic();
            int hashCode = WritableComparator.hashBytes(genetic, genetic.length);

            return (hashCode & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class GroupingComparator
    extends WritableComparator {
        BytesWritable.Comparator bytesWritableComparator = new BytesWritable.Comparator();

        public GroupingComparator() {
            super(GeneticWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int dl1 = readInt(b1, s1) * 8 + 4;
            int dl2 = readInt(b2, s2) * 8 + 4;

            return bytesWritableComparator.compare(b1, s1 + dl1, l1, b2, s2 + dl2, l2);
        }

    }

    static {
        WritableComparator.define(GeneticWritable.class, new Comparator());
    }
}
