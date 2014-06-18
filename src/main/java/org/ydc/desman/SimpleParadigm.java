package org.ydc.desman;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class SimpleParadigm
implements Paradigm {

    byte[] buffer = new byte[4096];
    ByteBuffer wrapper = ByteBuffer.wrap(buffer);
    final Random r = new Random();

    public void setParameters(String args[]) {
    };

    public Iterable<Contribution> genesis() {

        final Iterator iter = new Iterator<Contribution>() {
            Contribution contribution = new Contribution();
            int i = 0;

            public boolean hasNext() {
                return i < 10000;
            }

            public Contribution next() {
                long value = r.nextLong();
                i++;

                wrapper.putLong(0, value);
                byte[] individual = new byte[8];
                System.arraycopy(buffer, 0, individual, 0, 8);

                wrapper.putInt(0, i);
                byte[] population = new byte[4];
                System.arraycopy(buffer, 0, population, 0, 4);

                contribution.set(population, individual, new double[]{ value });
                return contribution;
            }

            public void remove() {
                i = 0;
            }
        };

        return new Iterable<Contribution>() {
            public Iterator<Contribution> iterator() { return iter; }
        };
    };

    public Contribution mutation(Contribution contribution) {
        int mutation = r.nextInt();
        System.arraycopy(contribution.getIndividual(), 0, buffer, 0, 8);
        long value = wrapper.getLong(0)-mutation;

        wrapper.putLong(0, value);
        byte[] individual = new byte[8];
        System.arraycopy(buffer, 0, individual, 0, 8);

        System.arraycopy(contribution.getPopulation(), 0, buffer, 0, 4);
        wrapper.putInt(0, wrapper.getInt(0) / 2);
        byte[] population = new byte[4];
        System.arraycopy(buffer, 0, population, 0, 4);

        contribution.set(population, individual, new double[]{ value });

        return contribution;
    };
    
    public Iterable<Contribution> generation(Iterable<Contribution> contributions) {
        final Iterator<Contribution> iter = contributions.iterator();

        final Iterator<Contribution> iter2 = new Iterator<Contribution>() {
            Contribution contribution = new Contribution();
            int i = 0;

            public boolean hasNext() {
                return i < 100 && iter.hasNext();
            }

            public Contribution next() {
                if(i % 4 == 0) {
                    Contribution c = iter.next();
                    contribution.set(c.getPopulation(), c.getIndividual(), c.getData());
                }
                i++;

                return contribution;
            }

            public void remove() {
                iter.remove();
                i = 0;
            }
        };

        return new Iterable<Contribution>() {
            public Iterator<Contribution> iterator() { return iter2; }
        };
    };
}
