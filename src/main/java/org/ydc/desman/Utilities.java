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

public class Utilities
        extends Configured
        implements Tool {

    Paradigm paradigm;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.printf("Usage: %s [generic options] cat|genesis arguments\n",
                    getClass().getSimpleName()
            );
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        String command = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        switch (command) {
            case "cat":
                return cat(args);

            case "genesis":
                return genesis(args);

            default:
                return -1;
        }

    }

    private int cat(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.printf("Usage: %s [generic options] cat file\n",
                    getClass().getSimpleName()
            );
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = this.getConf();

        Path path = new Path(args[0]);

        BytesWritable populationWritable = new BytesWritable();
        BytesWritable contributionWritable = new BytesWritable();
        GeneticHelper helper = new GeneticHelper();

        SequenceFile.Reader reader = null;

        try {
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

            while (reader.next(populationWritable, contributionWritable)) {
                byte[] population = populationWritable.copyBytes();
                byte[] individual = helper.getGenetic(contributionWritable);
                double[] data = helper.getData(contributionWritable);

                StringBuilder builder = new StringBuilder();

                for (int i = 0; i < population.length; i++) {
                    String num = Integer.toHexString(0xff & population[i]);
                    if (num.length() < 2) {
                        builder.append('0');
                    }
                    builder.append(num);
                }

                builder.append('\t');

                for (int i = 0; i < individual.length; i++) {
                    String num = Integer.toHexString(0xff & individual[i]);
                    if (num.length() < 2) {
                        builder.append('0');
                    }
                    builder.append(num);
                }

                System.out.printf("%s\t%s\n", builder.toString(), Arrays.toString(data));
            }
        } finally {
            IOUtils.closeStream(reader);
        }

        return 0;
    }

    private int genesis(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("Usage: %s [generic options] genesis <output> <paradigm class> <output> [<paradigm parameter>...]\n",
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
            paradigm = (Paradigm) paradigmClass.newInstance();
            paradigm.setParameters(paradigmArgs);
        } catch (InstantiationException | IllegalAccessException x) {
            throw new RuntimeException(x);
        }

        BytesWritable populationWritable = new BytesWritable();
        BytesWritable contributionWritable = new BytesWritable();
        GeneticHelper helper = new GeneticHelper();

        SequenceFile.Writer writer = null;

        try {
            writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(output),
                    SequenceFile.Writer.keyClass(populationWritable.getClass()),
                    SequenceFile.Writer.valueClass(contributionWritable.getClass()));

            Iterable<Contribution> contributions = paradigm.genesis();

            for (Contribution contribution : contributions) {
                byte[] population = contribution.getPopulation();
                byte[] individual = contribution.getIndividual();
                double[] data = contribution.getData();

                populationWritable.set(population, 0, population.length);

                helper.set(contributionWritable, individual, data);

                writer.append(populationWritable, contributionWritable);
            }
        } finally {
            IOUtils.closeStream(writer);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Utilities(), args);
        System.exit(exitCode);
    }
}
