package com.hsbc.random;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import static java.util.Objects.requireNonNull;

public final class AliasMethodProbabilisticRandomGen implements ProbabilisticRandomGen {

    private static final DoubleSupplier THREAD_LOCAL_SOURCE =
            () -> ThreadLocalRandom.current().nextDouble();

    private final int size;
    private final int[] numbers;
    private final double[] prob;
    private final int[] alias;
    private final DoubleSupplier randomSource;

    /**
     * Creates a generator that uses {@link ThreadLocalRandom}.
     *
     * @param samples numbers with their weights, not null, not empty
     * @throws IllegalArgumentException if the list is null, empty or has an invalid entry
     */
    public AliasMethodProbabilisticRandomGen(List<NumAndProbability> samples) {
        this(samples, THREAD_LOCAL_SOURCE);
    }

    /**
     * Creates a generator with a custom random source.
     *
     * @param sampleProbabilities numbers with their weights, not null, not empty
     * @param randomSource        returns values in [0, 1), not null
     * @throws NullPointerException     if the source is null
     * @throws IllegalArgumentException if the list is null, empty or has an invalid entry
     */
    public AliasMethodProbabilisticRandomGen(List<NumAndProbability> sampleProbabilities, DoubleSupplier randomSource) {
        this.randomSource = requireNonNull(randomSource, "randomSource");
        if (sampleProbabilities == null || sampleProbabilities.isEmpty()) {
            throw new IllegalArgumentException("sampleProbabilities must not be null or empty");
        }

        this.size = sampleProbabilities.size();
        this.numbers = new int[size];
        this.prob = new double[size];
        this.alias = new int[size];

        // Single pass: validate and copy to local arrays.
        double[] weights = new double[size];
        double total = 0.0;
        for (int i = 0; i < sampleProbabilities.size(); i++) {
            var numAndProbability = sampleProbabilities.get(i);
            validate(numAndProbability, i);
            numbers[i] = numAndProbability.getNumber();
            float probability = numAndProbability.getProbabilityOfSample();
            weights[i] = probability;
            total += probability;
        }

        // Vose's alias method. Worklists are int arrays used as stacks.
        double[] scaled = new double[size];
        int[] smallColumns = new int[size];
        int[] largeColumns = new int[size];
        int smallCount = 0;
        int largeCount = 0;
        for (int i = 0; i < size; i++) {
            scaled[i] = weights[i] * size / total;
            alias[i] = i;
            if (scaled[i] < 1.0) {
                smallColumns[smallCount++] = i;
            } else {
                largeColumns[largeCount++] = i;
            }
        }
        // Each step closes one small column, so the loop ends after at most N steps.
        while (smallCount > 0 && largeCount > 0) {
            int smallColumnNumIndex = smallColumns[--smallCount];
            int largeColumnNumIndex = largeColumns[--largeCount];

            // The small column keeps its own number with its own weight.
            // The large column gives the rest of the column: this is the alias.
            prob[smallColumnNumIndex] = scaled[smallColumnNumIndex];
            alias[smallColumnNumIndex] = largeColumnNumIndex;

            // The large column lost (1 - scaled[smallColumnNumIndex]) of its weight.
            double largeRemaining = scaled[largeColumnNumIndex] - (1 - scaled[smallColumnNumIndex]);
            scaled[largeColumnNumIndex] = largeRemaining;

            // The large column goes back to the stack that matches its new weight.
            if (largeRemaining < 1.0) {
                smallColumns[smallCount++] = largeColumnNumIndex;
            } else {
                largeColumns[largeCount++] = largeColumnNumIndex;
            }
        }
        // Floating point residue: the remaining columns are full.
        while (smallCount > 0) {
            prob[smallColumns[--smallCount]] = 1.0;
        }
        while (largeCount > 0) {
            prob[largeColumns[--largeCount]] = 1.0;
        }
    }

    @Override
    public int nextFromSample() {
        // The integer part picks the column. The fraction decides between the column and its alias.
        double position = randomSource.getAsDouble() * size;
        int column = (int) position;
        if (column >= size) {
            column = size - 1; // position can round up to size when the random value is very close to 1.0
        }
        double fraction = position - column;
        return fraction < prob[column] ? numbers[column] : numbers[alias[column]];
    }

    private static void validate(NumAndProbability numAndProbability, int index) {
        if (numAndProbability == null) {
            throw new IllegalArgumentException("sampleProbabilities[" + index + "] is null");
        }
        float probability = numAndProbability.getProbabilityOfSample();
        if (Float.isNaN(probability) || Float.isInfinite(probability) || probability <= 0f) {
            throw new IllegalArgumentException("sampleProbabilities[" + index + "] has an invalid probability: " + probability);
        }
    }
}
