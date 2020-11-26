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
        this.wordCounts = generateWordCounts(wordsIterable);
    }

    public TreeMap<String, Integer> generateWordCounts(Iterable<Text> wordsIterable) {
        String words = StreamSupport
                .stream(wordsIterable.spliterator(), false)
                .map(Text::toString)
                .collect(Collectors.joining(", "));

        return new TreeMap<>(Arrays
                .stream(words.split(", "))
                .collect(Collectors.groupingBy(Function.identity(), summingInt(s -> 1))));
    }

    public int getSize() {
        return this.wordCounts.size();
    }

    public String printWordCounts() {
        return this.wordCounts.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(" "));
    }
}
