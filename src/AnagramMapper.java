import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;


/**
 * AnagramMapper class. Extends Hadoop's Mapper class.
 */
public class AnagramMapper extends Mapper<Object, Text, AnagramCompositeKey, Text> {

    List<String> stopWords = getStopWords();

    /**
     * Mapper method.
     * <p>
     * Takes the given dataset and sorts each key alphabetically.
     * <p>
     * Sorted key and value is written to the given context.
     *
     * @param key     Given key.
     * @param value   Given value.
     * @param context Context to running job.
     * @throws IOException          Attempts to write given result.
     * @throws InterruptedException Attempts to write given result.
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        //Splits string into whitespace separated tokens for iteration.
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            //Uses a regular expression to remove:
            //
            // - All apostrophes that exist are the beginning
            //   and at the end of the word in a string are removed.
            //
            // - NOT all characters in a word along with NOT
            //   all apostrophes in a word. Meaning "you'll" is valid
            //   but "test'" will convert to "test".
            //
            // Word is even converted to lowercase.
            String currentWord = itr.nextToken()
                    .replaceAll("(^')|('$)|[^'a-zA-Z]", "")
                    .toLowerCase();

            // If stop word is found, don't write to context.
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
     *
     * @param word Character array of word to be sorted.
     * @return sorted character array as object Text.
     */
    private Text SortGivenWord(char[] word) {
        Arrays.sort(word);
        return new Text(new String(word));
    }

    /**
     * Gets stop words for filtering input data.
     *
     * @return List of stop words.
     */
    private List<String> getStopWords() {
        try {
            return stopWords = Arrays.asList(AnagramJobUtils.requestAndSaveStopWords());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

