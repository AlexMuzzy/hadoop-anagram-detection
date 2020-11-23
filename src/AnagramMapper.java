import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;


/**
 * AnagramMapper class. Extends Hadoop's Mapper class.
 */
public class AnagramMapper extends Mapper<Object, Text, AnagramCompositeKey, Text> {

    List<String> stopWords;

    {
        try {
            stopWords = Arrays.asList(JobUtils.requestAndSaveStopWords());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Mapper method.
     *
     * Takes the given dataset and sorts each key alphabetically.
     *
     * Sorted key and value is written to the given context.
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

//        Filter the given string tokenizer from the stop words request,
//        then parse the filtered words to construct the composite key
//        and context.

        while (itr.hasMoreTokens()) {

            String currentWord = itr.nextToken().replaceAll("[^a-zA-Z]", "");
            //grab string from iterable where
            if (stopWords.contains(currentWord)) continue;
            AnagramCompositeKey reducerKey = new AnagramCompositeKey(
                            SortGivenWord(currentWord.toCharArray()),
                            new IntWritable(1));

            context.write(
                    reducerKey,
                    new Text(currentWord));
        }
    }

    /**
     * Alphabetically sort word and return the result.
     * @param word Character array of word to be sorted.
     * @return sorted character array as object Text.
     */
    private Text SortGivenWord (char[] word){
        Arrays.sort(word);
        return new Text(new String(word));
    }
}

