import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;

/**
 * Anagram Reducer Class. Extends Hadoop's reducer class.
 */
public class AnagramReducer extends Reducer<AnagramCompositeKey, Text, Text, Text> {

    /**
     * Reducer method.
     * Takes each key value pair and summarises every value to each distinct key.
     *
     * @param key Each given key.
     * @param values Every given value.
     * @param context Context of running job.
     * @throws IOException Attempts to write given result.
     * @throws InterruptedException Attempts to write given result.
     */
    public void reduce(AnagramCompositeKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder anagram = null;
        int endCount = 0;
        for (Text val : values) {
            if (anagram == null) {
                anagram = Optional.ofNullable(val.toString()).map(StringBuilder::new).orElse(null);
            } else {
                anagram.append(", ").append(val.toString());
            }
            endCount += key.getFrequency().get();
        }

        if (key.getFrequency().get() > 1) {//Check that there are more than one values to display the anagram.
            assert anagram != null;
            context.write(new Text(
                    key.getKeyName().toString() + " " + endCount),
                    FormatAnagram(anagram));
        }
    }

    private Text FormatAnagram(StringBuilder anagramSet) {
        return new Text(" { " + anagramSet.toString() + " }");
    }
}

