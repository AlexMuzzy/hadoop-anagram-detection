import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.summingInt;

public class AnagramCompositeValues {

    Map<String, Integer> wordCounts;

    public AnagramCompositeValues(Iterable<Text> wordsIterable) {
        this.wordCounts = generateWordCounts(wordsIterable);
    }


    public Map<String, Integer> generateWordCounts(Iterable<Text> wordsIterable) {
        String words = StreamSupport
                .stream(wordsIterable.spliterator(), false)
                .map(Text::toString)
                .collect(Collectors.joining(", "));

        return Arrays
                .stream(words.split(", "))
                .collect(Collectors.groupingBy(s -> s, summingInt(s -> 1)));
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
