import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.omg.CORBA.StringHolder;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Anagram Reducer Class. Extends Hadoop's reducer class.
 */
public class AnagramReducer extends Reducer<AnagramCompositeKey, Text, Text, Text> {

    private TreeMap<AnagramCompositeKey, String> anagramMap;

    public static Integer[] optionalParamsValues;


    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();

        optionalParamsValues = AnagramJobUtils.getOptionals(new String[]{
                conf.get("keyName.descending"),
                conf.get("frequency.descending")
        });

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

        int anagramSize = anagram.getSize();

        // If there is more than one unique word in the anagram set, continue.
        if (anagramSize > 1) {
            anagramMap.put(new AnagramCompositeKey(
                            new Text(anagram.getFirstKey()),
                            new IntWritable(anagramSize)),
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

