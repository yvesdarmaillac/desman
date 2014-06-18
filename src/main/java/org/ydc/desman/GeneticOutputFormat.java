package org.ydc.desman;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class GeneticOutputFormat
extends SequenceFileOutputFormat<BytesWritable, BytesWritable> {

    @Override
    public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {

        final SequenceFile.Writer out = getSequenceWriter(context, BytesWritable.class, BytesWritable.class);

        return new RecordWriter<BytesWritable, BytesWritable>() {

            public void write(BytesWritable key, BytesWritable value)
                throws IOException {

                    out.append(key, value);
                }

            public void close(TaskAttemptContext context) throws IOException { 
                out.close();
            }
        };

    }
}
