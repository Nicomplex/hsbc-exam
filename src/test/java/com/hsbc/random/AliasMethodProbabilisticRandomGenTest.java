package com.hsbc.random;

import com.hsbc.random.ProbabilisticRandomGen.NumAndProbability;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.function.DoubleSupplier;

import static org.assertj.core.api.Assertions.*;

class AliasMethodProbabilisticRandomGenTest {

    private static NumAndProbability np(int number, float p) {
        return new NumAndProbability(number, p);
    }

    private static List<NumAndProbability> list(NumAndProbability... items) {
        return new ArrayList<>(Arrays.asList(items));
    }

    private static AliasMethodProbabilisticRandomGen gen(DoubleSupplier source, NumAndProbability... items) {
        return new AliasMethodProbabilisticRandomGen(list(items), source);
    }

    @Nested
    class Validation {

        @Test
        void rejects_null_list() {
            assertThatThrownBy(() -> new AliasMethodProbabilisticRandomGen(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void rejects_empty_list() {
            assertThatThrownBy(() -> new AliasMethodProbabilisticRandomGen(list()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void rejects_null_element_and_names_index() {
            assertThatThrownBy(() -> new AliasMethodProbabilisticRandomGen(list(np(1, 1f), null)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sampleProbabilities[1]");
        }

        @ParameterizedTest
        @ValueSource(floats = {0f, -0.1f, -Float.MIN_VALUE, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY})
        void rejects_invalid_probability_and_names_index(float bad) {
            assertThatThrownBy(() -> new AliasMethodProbabilisticRandomGen(
                    list(np(1, 0.5f), np(2, bad))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sampleProbabilities[1]");
        }

        @Test
        void rejects_null_random_source() {
            List<NumAndProbability> samples = list(np(1, 1f));
            assertThatNullPointerException()
                    .isThrownBy(() -> new AliasMethodProbabilisticRandomGen(samples, null));
        }
    }

    @Nested
    class Deterministic {

        // 0.9999999999999999 is Math.nextDown(1.0), the largest double below 1.
        @ParameterizedTest
        @ValueSource(doubles = {0.0, 0.5, 0.9999999999999999})
        void single_element_is_always_returned(double u) {
            assertThat(gen(() -> u, np(42, 0.3f)).nextFromSample()).isEqualTo(42);
        }

        @Test
        void equal_weights_map_columns_in_order() {
            double[] cursor = {0.0};
            var g = gen(() -> cursor[0], np(10, 1f), np(20, 1f), np(30, 1f), np(40, 1f));
            double[] inputs = {0.0, 0.2499, 0.25, 0.4999, 0.5, 0.7499, 0.75, Math.nextDown(1.0)};
            int[] expected = {10, 10, 20, 20, 30, 30, 40, 40};
            for (int i = 0; i < inputs.length; i++) {
                cursor[0] = inputs[i];
                assertThat(g.nextFromSample()).as("u=%s", inputs[i]).isEqualTo(expected[i]);
            }
        }

        @Test
        void unequal_two_element_table_matches_hand_computation() {
            // scaled = [0.5, 1.5]: column 0 has prob 0.5 and alias 1, column 1 is full.
            double[] cursor = {0.0};
            var g = gen(() -> cursor[0], np(1, 0.25f), np(2, 0.75f));
            double[] inputs = {0.0, 0.24, 0.25, 0.4999, 0.5, 0.9, Math.nextDown(1.0)};
            int[] expected = {1, 1, 2, 2, 2, 2, 2};
            for (int i = 0; i < inputs.length; i++) {
                cursor[0] = inputs[i];
                assertThat(g.nextFromSample()).as("u=%s", inputs[i]).isEqualTo(expected[i]);
            }
        }

        @Test
        void defensive_copy_ignores_later_mutation() {
            List<NumAndProbability> source = list(np(1, 0.5f), np(2, 0.5f));
            double[] cursor = {0.0};
            var g = new AliasMethodProbabilisticRandomGen(source, () -> cursor[0]);
            source.clear();
            source.add(np(99, 1f));
            assertThat(g.nextFromSample()).isEqualTo(1);
            cursor[0] = 0.75;
            assertThat(g.nextFromSample()).isEqualTo(2);
        }

        @Test
        void unnormalized_weights_give_same_result_as_normalized() {
            double[] cursor = {0.0};
            var raw = gen(() -> cursor[0], np(1, 1f), np(2, 3f));
            var normalized = gen(() -> cursor[0], np(1, 0.25f), np(2, 0.75f));
            for (int i = 0; i < 1000; i++) {
                cursor[0] = i / 1000.0;
                assertThat(raw.nextFromSample()).isEqualTo(normalized.nextFromSample());
            }
        }
    }

    @Nested
    class Statistical {

        @Test
        void empirical_frequencies_match_probabilities() {
            var samples = list(np(7, 0.1f), np(1, 0.4f), np(7, 0.2f), np(5, 0.3f));
            var random = new SplittableRandom(12345L);
            var g = new AliasMethodProbabilisticRandomGen(samples, random::nextDouble);
            int total = 1_000_000;
            Map<Integer, Integer> counts = new HashMap<>();
            for (int i = 0; i < total; i++) {
                counts.merge(g.nextFromSample(), 1, Integer::sum);
            }
            Map<Integer, Double> expected = Map.of(7, 0.3, 1, 0.4, 5, 0.3);
            assertThat(counts.keySet()).isEqualTo(expected.keySet());
            expected.forEach((number, p) -> {
                double freq = counts.get(number) / (double) total;
                double tolerance = 4 * Math.sqrt(p * (1 - p) / total);
                assertThat(freq).as("number %d", number).isCloseTo(p, within(tolerance));
            });
        }
    }

    @Nested
    class DefaultConstructor {

        @Test
        void returns_only_input_numbers() {
            var g = new AliasMethodProbabilisticRandomGen(
                    list(np(3, 0.2f), np(5, 0.3f), np(8, 0.5f)));
            Set<Integer> allowed = Set.of(3, 5, 8);
            for (int i = 0; i < 10_000; i++) {
                assertThat(g.nextFromSample()).isIn(allowed);
            }
        }
    }
}
