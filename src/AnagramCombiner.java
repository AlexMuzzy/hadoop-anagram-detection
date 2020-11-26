import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnagramCombiner extends Reducer<AnagramCompositeKey, Text, AnagramCompositeKey, Text> {

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

        int count = 0;

        StringBuilder anagram = null;
        for (Text val : values) {
            if (anagram == null) {
                anagram = new StringBuilder(val.toString());
            } else {
                anagram.append(", ").append(val.toString());
            }
            count++;
        }

        key.setFrequency(new IntWritable(count));

        if (anagram != null) {
            context.write(key, new Text(anagram.toString()));
        }
    }
}
