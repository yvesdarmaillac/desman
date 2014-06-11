package org.ydc.desman;

public interface Paradigm {
    void setParameters(String args[]);

    Iterable<Contribution> genesis();
    Contribution mutation(Contribution contribution);
    Iterable<Contribution> generation(Iterable<Contribution> contribution);
}
