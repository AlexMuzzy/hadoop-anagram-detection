import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Anagram Reducer Class. Extends Hadoop's reducer class.
 */
public class AnagramReducer extends Reducer<AnagramCompositeKey, Text, Text, Text> {

    private TreeMap<AnagramCompositeKey, AnagramCompositeValues> anagramMap;

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

        boolean keyFoundFlag = false;

        // Iterate through each key, to see if there is a matching key name in the
        // composite key to the current key in the reducer.
        for (AnagramCompositeKey anagramKey: anagramMap.keySet()) {
            if(anagramKey.getKeyName().equals(key.getKeyName())) {
                // Checks if there is a mutual key contained in the anagramMap.
                if (anagramMap.get(anagramKey) != null) {
                    anagramMap.get(anagramKey).addWordCounts(anagram);
                } else {
                    anagramMap.put(key, anagram);
                }
                // Merge the two composite values.
                AnagramCompositeValues anagramCompositeValues = anagramMap.get(anagramKey);
                anagramMap.remove(anagramKey);
                //Change key to match new word count.
                anagramMap.put(new AnagramCompositeKey(key.getKeyName(),
                        new IntWritable(anagramCompositeValues.getSize())),
                        anagramCompositeValues);

                //set the new distinct word count size.
                keyFoundFlag = true;
            }
        }

        // If the current key was merged into an already existing key,
        // then continue to the next iteration.
        if(!keyFoundFlag) {
            anagramMap.put(new AnagramCompositeKey(
                            new Text(key.getKeyName()),
                            new IntWritable(anagram.getSize())),
                    anagram);
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

        anagramMap.forEach((key, value) -> key.setKeyName(new Text(value.getFirstKey())));

        anagramMap.forEach((key, value) -> {
            try {
                if (value.getSize() > 1) //Check to see if there is more than one distinct word.
                    context.write(key.getKeyPair(), new Text(" { " + value.printWordCounts() + " } "));

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}

