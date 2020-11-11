import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class AnagramMapper extends Mapper<Object, Text, Text, Text> {

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

    private String SortGivenWord (char[] word){

        Arrays.sort(word);
        return Arrays.toString(word);
    }
}

