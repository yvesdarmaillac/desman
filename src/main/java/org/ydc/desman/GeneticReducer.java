package org.ydc.desman;

import java.io.IOException;
import java.lang.Iterable;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.BytesWritable;

public class GeneticReducer
extends Reducer<GeneticWritable, GeneticWritable, BytesWritable, GeneticWritable> { 

    Paradigm paradigm;

    final GeneticWritable contributionWritable = new GeneticWritable();
    final BytesWritable populationWritable = new BytesWritable();

    static class ContributionIterable implements Iterable<Contribution>, Iterator<Contribution> {
        byte[] population;
        Iterator<GeneticWritable> iter;
        Contribution contribution = new Contribution();

        public void set(GeneticWritable key, Iterable<GeneticWritable> value) {
            this.population = key.getGenetic();
            this.iter = value.iterator();
        }

        public Iterator<Contribution> iterator() {
            return this;
        }

        public boolean hasNext() {
            return iter.hasNext();
        }

        public Contribution next() {
            GeneticWritable geneticWritable = iter.next();
            byte[] individual = geneticWritable.getGenetic();
            double[] data = geneticWritable.getData();

            contribution.set(individual, population, data);

            return contribution;
        }

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

        } catch (InstantiationException x) {
            throw new RuntimeException(x);

        } catch (IllegalAccessException x) {
            throw new RuntimeException(x);

        }
    }

    @Override
    public void reduce(GeneticWritable key, Iterable<GeneticWritable> value, Context context)
    throws IOException, InterruptedException {

        contributionIterable.set(key, value);
        
        Iterable<Contribution> contributions = paradigm.generation(contributionIterable);

        for(Contribution contribution: contributions) {
            byte[] individual = contribution.getIndividual();
            byte[] population = contribution.getPopulation();
            double[] data = contribution.getData();

            populationWritable.set(population, 0,  population.length);

            contributionWritable.setGenetic(individual);
            contributionWritable.setData(data);

            context.write(populationWritable, contributionWritable);
        }
    }
}
