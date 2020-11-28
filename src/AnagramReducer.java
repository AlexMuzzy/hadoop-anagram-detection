import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Anagram Reducer Class. Extends Hadoop's reducer class.
 */
public class AnagramReducer extends Reducer<AnagramCompositeKey, Text, Text, Text> {

    private TreeMap<AnagramCompositeKey, String> anagramMap;

    @Override
    public void setup(Context context) {
        anagramMap = new TreeMap<>();
    }

    /**
     * Reducer method.
     * Takes each key value pair and summarises every value to each distinct key within.
     * the given anagram composite key class.
     *
     * @param key     Each given key.
     * @param values  Every given value.
     * @param context Context of running job.
     */
    public void reduce(AnagramCompositeKey key, Iterable<Text> values, Context context) {

        AnagramCompositeValues anagram = new AnagramCompositeValues(values);

        // If there is more than one unique word in the anagram set, continue.
        if (anagram.getSize() > 1) {
            anagramMap.put(new AnagramCompositeKey(
                            key.getKeyName(),
                            key.getFrequency()),
                    anagram.printWordCounts());
        }
    }

    /**
     * Cleanup Method.
     * Writes each key value pair to the resultant output. Formats the output
     * with curly braces.
     *
     * @param context Given context.
     */
    public void cleanup(Context context) {
        anagramMap.forEach((key, value) -> {
            try {
                context.write(key.getKeyPair(), new Text(" { " + value + " } "));

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }


}

