import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;

public class AnagramReducer extends Reducer<Text, Text, Text, Text> {

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

