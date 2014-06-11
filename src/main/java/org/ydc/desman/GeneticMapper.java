package org.ydc.desman;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GeneticMapper
extends Mapper<BytesWritable, GeneticWritable, GeneticWritable, GeneticWritable>
{
    Paradigm paradigm;
    Contribution contribution = new Contribution();
    final GeneticWritable populationWritable = new GeneticWritable();
    final GeneticWritable contributionWritable = new GeneticWritable();

    @Override
    public void setup(Mapper<BytesWritable, GeneticWritable, GeneticWritable, GeneticWritable>.Context context)
    {
        Configuration conf = context.getConfiguration();
        Class<?> paradigmClass = conf.getClass("paradigm.class", SimpleParadigm.class);
        String[] paradigmArgs = conf.getStrings("paradigm.args");
        try
        {
            this.paradigm = ((Paradigm)paradigmClass.newInstance());
            this.paradigm.setParameters(paradigmArgs);
        }
        catch (InstantiationException x)
        {
            throw new RuntimeException(x);
        }
        catch (IllegalAccessException x)
        {
            throw new RuntimeException(x);
        }
    }

    @Override
    public void map(BytesWritable key, GeneticWritable value, Context context)
        throws IOException, InterruptedException
        {
            byte[] population = key.getBytes();
            byte[] individual = value.getGenetic();
            double[] data = value.getData();

            this.contribution.set(individual, population, data);
            this.contribution = this.paradigm.mutation(this.contribution);

            this.populationWritable.setGenetic(this.contribution.getPopulation());
            this.populationWritable.setData(this.contribution.getData());

            this.contributionWritable.setGenetic(this.contribution.getIndividual());
            this.contributionWritable.setData(this.contribution.getData());

            context.write(this.populationWritable, this.contributionWritable);
        }
}
