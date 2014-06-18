package org.ydc.desman;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GeneticMapper
extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable>
{
    Paradigm paradigm;
    Contribution contribution = new Contribution();

    final GeneticHelper helper = new GeneticHelper();

    final BytesWritable populationWritable = new BytesWritable();
    final BytesWritable contributionWritable = new BytesWritable();

    @Override
    public void setup(Context context)
    {
        Configuration conf = context.getConfiguration();
        Class<?> paradigmClass = conf.getClass("paradigm.class", SimpleParadigm.class);
        String[] paradigmArgs = conf.getStrings("paradigm.args");

        try
        {
            this.paradigm = ((Paradigm)paradigmClass.newInstance());
            this.paradigm.setParameters(paradigmArgs);
        }
        catch (InstantiationException | IllegalAccessException x)
        {
            throw new RuntimeException(x);
        }
    }

    @Override
    public void map(BytesWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException
        {
            byte[] population = key.copyBytes();
            byte[] individual = helper.getGenetic(value);
            double[] data = helper.getData(value);

            this.contribution.set(population, individual, data);
            this.contribution = this.paradigm.mutation(this.contribution);

            this.helper.set(this.populationWritable, this.contribution.getPopulation(), this.contribution.getData()[0]);

            this.helper.set(this.contributionWritable, this.contribution.getIndividual(), this.contribution.getData());

            context.write(this.populationWritable, this.contributionWritable);
        }
}
