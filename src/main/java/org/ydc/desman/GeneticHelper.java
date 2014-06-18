package org.ydc.desman;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GeneticHelper {
    final byte[] buffer = new byte[8192];
    final ByteBuffer wrapper = ByteBuffer.wrap(buffer);

    public byte[] getGenetic(BytesWritable writable) {
        wrapper.clear();
        System.arraycopy(writable.getBytes(), 0, buffer, 0, writable.getLength());

        byte[] genetic = new byte[wrapper.getInt()];

        wrapper.position(8);
        wrapper.get(genetic);

        return genetic;
    }

    public double[] getData(BytesWritable writable) {
        wrapper.clear();
        System.arraycopy(writable.getBytes(), 0, buffer, 0, writable.getLength());

        int offset = wrapper.getInt();
        double[] data = new double[wrapper.getInt()];

        wrapper.position(8 + offset);
        
        for(int i = 0; i < data.length; i++) {
            data[i] = wrapper.getDouble();
        }

        return data;
    }

    public void set(BytesWritable writable, byte[] genetic, double[] data) {
        wrapper.clear();
        wrapper.putInt(genetic.length);
        wrapper.putInt(data.length);
        wrapper.put(genetic);
        for(int i = 0; i < data.length; i++) {
            wrapper.putDouble(data[i]);
        }

        writable.set(buffer, 0, wrapper.position());
    }

    public void set(BytesWritable writable, byte[] genetic, double data) {
        wrapper.clear();
        wrapper.putInt(genetic.length);
        wrapper.putInt(1);
        wrapper.put(genetic);
        wrapper.putDouble(data);

        writable.set(buffer, 0, wrapper.position());
    }

    public String toString() {
        return buffer.toString();
    }

    public static class Comparator
    extends WritableComparator {
        public Comparator() {
            super(BytesWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            s1+= 4;
            s2+= 4;
            int gl1 = readInt(b1, s1);
            int gl2 = readInt(b2, s2);

            s1+= 4;
            s2+= 4;
            int dl1 = readInt(b1, s1) * 8;
            int dl2 = readInt(b2, s2) * 8;

            s1+= 4;
            s2+= 4;
            int result = compareBytes(b1, s1, gl1, b2, s2, gl2);

            s1+= gl1;
            s2+= gl2;
            for(int i = 0; i < dl1 && i < dl2 && result == 0; i+= 8) {
                double data1 = readDouble(b1, s1 + i);
                double data2 = readDouble(b2, s2 + i);

                result = Double.compare(data1, data2);
            }

            return result;
        }
    }


    public static class Partitioner<T>
    extends org.apache.hadoop.mapreduce.Partitioner<BytesWritable, T> {
        final GeneticHelper helper = new GeneticHelper();

        @Override
        public int getPartition(BytesWritable key, T value, int numPartitions) {
            byte[] genetic = helper.getGenetic(key);
            int hashCode = WritableComparator.hashBytes(genetic, genetic.length);

            return (hashCode & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class GroupingComparator
    extends WritableComparator {
        BytesWritable.Comparator bytesWritableComparator = new BytesWritable.Comparator();

        public GroupingComparator() {
            super(BytesWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            s1+= 4;
            s2+= 4;
            int gl1 = readInt(b1, s1);
            int gl2 = readInt(b2, s2);

            s1+= 8;
            s2+= 8;
            return compareBytes(b1, s1, gl1, b2, s2, gl2);
        }
    }
}
