package org.ydc.desman;

/**
 * In an evolutionary algorithm, an individual contribute to a population ; for mutation, selection or reproduction.
 *
 * @see Paradigm
 * @author ydc
 */
public final class Contribution {

    byte[] individual;
    byte[] population;
    double[] data;

    /**
     * Builds an instance of a {@link Contribution}.
     */
    public Contribution() {
    }

    /**
     * Set contribution properties.
     *
     * @param population The population to which the individual will contribute.
     * @param individual The individual that contribute.
     * @param data the data used by an evolutionary algorithm to compute reproduction or mutation.
     */
    public void set(byte[] population, byte[] individual, double[] data) {
        this.individual = individual;
        this.population = population;
        this.data = data;
    }

    /**
     * The individual which contribute to reproduction or mutation.
     *
     * @return the individual.
     */
    public byte[] getIndividual() {
        return this.individual;
    }

    /**
     * The population which the individual contributes to.
     *
     * @return the data.
     */
    public byte[] getPopulation() {
        return this.population;
    }

    /**
     * The data needed by an evolutionary algorithm to compute mutation or reproduction.
     *
     * @return the data.
     */
    public double[] getData() {
        return this.data;
    }
}
