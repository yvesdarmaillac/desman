package org.ydc.desman;

public class Contribution {
    byte[] individual;
    byte[] population;
    double[] data;

    public void set(byte[] individual, byte[] population, double[] data) {
        this.individual = individual;
        this.population = population;
        this.data = data;
    }

    public byte[] getIndividual() {
        return this.individual;
    }

    public byte[] getPopulation() {
        return this.population;
    }

    public double[] getData() {
        return this.data;
    }
}
