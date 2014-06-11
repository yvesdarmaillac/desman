package org.ydc.desman;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeneticDriver
extends Configured
implements Tool {

    Paradigm paradigm;

    @Override
    public int run(String[] args) throws Exception {
        if(args.length < 3) {
            System.err.printf("Usage: %s [generic options] <input> <output> <paradigm class> [<paradigm parameter>...]\n",
                getClass().getSimpleName()
            );
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = this.getConf();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Class<?> paradigmClass = Class.forName(args[2]);
        String[] paradigmArgs = Arrays.copyOfRange(args, 3, args.length);

        conf.setClass("paradigm.class", paradigmClass, Paradigm.class);
        conf.setStrings("paradigm.args", paradigmArgs);

        Job job = Job.getInstance(conf, "Genetic optimization");
        job.setJarByClass(getClass());

        job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
        FileInputFormat.addInputPath(job, input);

        job.setMapperClass(GeneticMapper.class);
        job.setMapOutputKeyClass(GeneticKeyWritable.class);
        job.setMapOutputValueClass(GeneticKeyWritable.class);

        job.setPartitionerClass(GeneticPartitioner.class);
        job.setGroupingComparatorClass(GeneticPartitioner.class);

        job.setReducerClass(GeneticReducer.class);
        job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GeneticDriver(), args);
        System.exit(exitCode);
    }
}
