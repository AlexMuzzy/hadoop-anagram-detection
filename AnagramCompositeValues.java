import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.summingInt;

public class AnagramCompositeValues {

    TreeMap<String, Integer> wordCounts;

    /**
     * Constructor. Gets Iterable object and set wordCounts to given
     * treemap through its builtin method generateWordCounts.
     *
     * @param wordsIterable Iterable object of words.
     */
    public AnagramCompositeValues(Iterable<Text> wordsIterable) {

        String words = getWordsFromIterable(wordsIterable);
        this.wordCounts = generateWordCounts(words);
    }

    /**
     * Uses the Iterable object that is parsed through the constructor
     * to generate a concatenated string of the iterated Text objects.
     *
     * @param wordsIterable Iterable Text object of words.
     * @return Concatenated string of words.
     */
    private String getWordsFromIterable(Iterable<Text> wordsIterable) {
        return StreamSupport
                .stream(wordsIterable.spliterator(), false) // Split Iterable.
                .map(Text::toString) // Map each Text object to string.
                .collect(Collectors.joining(", "));
        // Join via delimiter ", ".
    }

    /**
     * Produces a TreeMap (sorted map) of the given concatenated words.
     * Splits each word into an array, which then gets compared and grouped.
     * <p>
     * Count value is produced with each word given the value of 1
     * and summed together.
     *
     * @param words String of concatenated words.
     * @return TreeMap of each word and its occurrence.
     */
    private TreeMap<String, Integer> generateWordCounts(String words) {

        return new TreeMap<>(Arrays
                .stream(words.split(", ")) // Split each word via delimiter ", "
                .collect(Collectors.groupingBy(Function.identity(), summingInt(s -> 1))));
        // Collect each word together, with the rules that they are summarised with
        // the respective value of 1 via equality check.
    }

    public int getSize() {
        return this.wordCounts.size();
    }

    public String getFirstKey () {
        return this.wordCounts.firstKey();
    }

    /**
     * Formats the word counts within the map and prints each
     * key value pair iteratively.
     *
     * @return String of printed word counts.
     */
    public String printWordCounts() {

        return this.wordCounts.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(" "));
    }
}
