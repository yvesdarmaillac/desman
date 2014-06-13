package org.ydc.desman;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Utilities
extends Configured
implements Tool {

    Paradigm paradigm;

    @Override
    public int run(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.printf("Usage: %s [generic options] [cat] <file>\n",
                getClass().getSimpleName()
            );
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = this.getConf();

        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);

        BytesWritable populationWritable = new BytesWritable();
        BytesWritable contributionWritable = new BytesWritable();
        GeneticHelper helper = new GeneticHelper();

        SequenceFile.Reader reader = null;

        try {
            reader = new SequenceFile.Reader(fs, path, conf);

            while(reader.next(populationWritable, contributionWritable)) {
                byte[] individual = helper.getGenetic(contributionWritable);
                byte[] population = populationWritable.copyBytes();
                double[] data = helper.getData(contributionWritable);
                
                System.out.printf("%s\t%s\n", populationWritable.toString(), contributionWritable.toString());
            }
        } finally {
            IOUtils.closeStream(reader);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Utilities(), args);
        System.exit(exitCode);
    }
}
