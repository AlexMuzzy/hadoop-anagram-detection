import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;


/**
 * AnagramMapper class. Extends Hadoop's Mapper class.
 */
public class AnagramMapper extends Mapper<Object, Text, Text, Text> {

    /**
     * Mapper method.
     *
     * Takes the given dataset and sorts each key alphabetically.
     *
     * Given sorted key and value is written to the given context.
     *
     * @param key Given key.
     * @param value Given value.
     * @param context Context to running job.
     * @throws IOException Attempts to write given result.
     * @throws InterruptedException Attempts to write given result.
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            String currentWord = itr.nextToken();
            context.write(
                    new Text(SortGivenWord(currentWord.toCharArray())),
                    new Text(currentWord));
        }
    }

    /**
     * Alphabetically sort word and return the result.
     * @param word Character array of word.
     * @return sorted character array.
     */
    private String SortGivenWord (char[] word){

        Arrays.sort(word);
        return Arrays.toString(word);
    }
}

