package org.ydc.desman;

import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeneticInitializer
extends Configured
implements Tool {

    Paradigm paradigm;

    @Override
    public int run(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.printf("Usage: %s [generic options] <output> <paradigm class> [<paradigm parameter>...]\n",
                getClass().getSimpleName()
            );
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = this.getConf();

        Path output = new Path(args[0]);
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);

        Class<?> paradigmClass = Class.forName(args[1]);
        String[] paradigmArgs = Arrays.copyOfRange(args, 2, args.length);

        try {
            paradigm = (Paradigm)paradigmClass.newInstance();
            paradigm.setParameters(paradigmArgs);
        } catch (InstantiationException x) {
            throw new RuntimeException(x);
        } catch (IllegalAccessException x) {
            throw new RuntimeException(x);
        }

        BytesWritable populationWritable = new BytesWritable();
        BytesWritable contributionWritable = new BytesWritable();
        GeneticHelper helper = new GeneticHelper();

        SequenceFile.Writer writer = null;

        try {
            writer = SequenceFile.createWriter(fs, conf, output, populationWritable.getClass(), contributionWritable.getClass());

            Iterable<Contribution> contributions = paradigm.genesis();

            for(Contribution contribution: contributions) {
                byte[] population = contribution.getPopulation();
                byte[] individual = contribution.getIndividual();
                double[] data = contribution.getData();
                
                populationWritable.set(population, 0,  population.length);

                helper.set(contributionWritable, individual, data);

                writer.append(populationWritable, contributionWritable);
            }
        } finally {
            IOUtils.closeStream(writer);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GeneticInitializer(), args);
        System.exit(exitCode);
    }
}
