import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Anagram Reducer Class. Extends Hadoop's reducer class.
 */
public class AnagramReducer extends Reducer<AnagramCompositeKey, Text, Text, Text> {

    private HashMap<String, AnagramCompositeValues> anagramMap;

    @Override
    public void setup(Context context) {
        anagramMap = new HashMap<>();
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

        if(anagramMap.containsKey(key.getKeyName().toString())) {
            anagramMap.get(key.getKeyName().toString()).addWordCounts(anagram);
        } else {
            anagramMap.put(key.getKeyName().toString(), anagram);
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
        // Merge each key value pair where key name matches since its currently possible to
        // have pairs where the name matches, but its distinct count is different.

        TreeMap<AnagramCompositeKey, AnagramCompositeValues> finalAnagramMap = new TreeMap<>();

        //Create new treemap so keys are sorted based on first word in set.
        anagramMap.forEach((key, value) ->
                finalAnagramMap.put(new AnagramCompositeKey(
                new Text(value.getFirstKey()),
                new IntWritable(value.getSize())),
                value));

        finalAnagramMap.forEach((key, value) -> {
            try {
                if (value.getSize() > 1) //Check to see if there is more than one distinct word.
                    context.write(key.getKeyPair(), new Text(" { " + value.printWordCounts() + " } "));

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}

