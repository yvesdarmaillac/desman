package org.ydc.desman;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class SimpleParadigm
implements Paradigm {

    public void setParameters(String args[]) {
    };

    public Iterable<Contribution> genesis() {

        final Iterator iter = new Iterator<Contribution>() {
            Contribution contribution;
            int i = 0;
            Random r= new Random();

            public boolean hasNext() {
                return i < 10000;
            }

            public Contribution next() {
                byte[] result = new byte[4];
                i++;
                r.nextBytes(result);
                contribution.set(result, result, new double[]{ 1 });
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
        return contribution;
    };
    
    public Iterable<Contribution> generation(Iterable<Contribution> contributions) {
        return contributions;
    };
}
