package org.ydc.desman;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.BytesWritable;

public class GeneticReducer
extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> { 

    Paradigm paradigm;

    final BytesWritable contributionWritable = new BytesWritable();
    final BytesWritable populationWritable = new BytesWritable();

    final GeneticHelper helper = new GeneticHelper();

    double dataMin = Double.MAX_VALUE;

    static class ContributionIterable implements Iterable<Contribution>, Iterator<Contribution> {
        byte[] population;
        Iterator<BytesWritable> iter;
        final Contribution contribution = new Contribution();
        final GeneticHelper helper = new GeneticHelper();

        public void set(BytesWritable key, Iterable<BytesWritable> value) {
            this.population = helper.getGenetic(key);
            this.iter = value.iterator();
        }

        @Override
        public Iterator<Contribution> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Contribution next() {
            BytesWritable writable = iter.next();
            byte[] individual = helper.getGenetic(writable);
            double[] data = helper.getData(writable);

            contribution.set(population, individual, data);

            return contribution;
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    final ContributionIterable contributionIterable = new ContributionIterable();

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        Class<?> paradigmClass = conf.getClass("paradigm.class", SimpleParadigm.class);
        String[] paradigmArgs = conf.getStrings("paradigm.args");

        try {
            paradigm = (Paradigm)paradigmClass.newInstance();
            paradigm.setParameters(paradigmArgs);

        } catch (InstantiationException | IllegalAccessException x) {
            throw new RuntimeException(x);
        }
    }

    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> value, Context context)
    throws IOException, InterruptedException {

        contributionIterable.set(key, value);
        
        Iterable<Contribution> contributions = paradigm.generation(contributionIterable);

        for(Contribution contribution: contributions) {
            byte[] individual = contribution.getIndividual();
            byte[] population = contribution.getPopulation();
            double[] data = contribution.getData();

            populationWritable.set(population, 0,  population.length);

            this.helper.set(this.contributionWritable, individual, data);

            context.write(populationWritable, contributionWritable);

            if(data[0] < dataMin) {
                dataMin = data[0];
            }
        }
    }
}
