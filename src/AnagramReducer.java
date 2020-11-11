import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;

/**
 * Anagram Reducer Class. Extends Hadoop's reducer class.
 */
public class AnagramReducer extends Reducer<Text, Text, Text, Text> {

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
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder anagram = null;
        for (Text val : values) {
            if (anagram == null) {
                anagram = Optional.ofNullable(val.toString()).map(StringBuilder::new).orElse(null);
            } else {
                anagram.append(", ").append(val.toString());
            }
        }
        context.write(key, new Text(anagram == null ? null : anagram.toString()));


    }
}

