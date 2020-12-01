import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * A class containing all common utility methods that are used for the function
 * of this project.
 */
public class AnagramJobUtils {

    /**
     * Checks if an output directory has been created.hot chocolate with bunny
     * If it is created, delete it.
     *
     * @param outputString String of output directory.
     */
    public static boolean checkOutputDirectory(String outputString) throws IOException {

        Path outputPath = new Path(outputString);
        FileSystem hdfs = FileSystem.get(new Configuration());

        //print continuing job message.
        if (!hdfs.exists(outputPath)) {
            System.out.println("Output Directory Not Found, continuing job.");
            return false;
        }

        //print output directory has been found along with requests user's input.
        System.out.println("Directory: " +
                outputPath +
                " has been found. Delete it to re-run current job?");

        System.out.print("Press enter for yes: ");
        Scanner scan = new Scanner(System.in);
        String userInputString = scan.nextLine();
        scan.close();//Close the input scanner.

        //if output directory exists, delete.
        if (userInputString.isEmpty()) {
            hdfs.delete(outputPath, true);// Delete the given folder.
        }

        return true;
    }

    /**
     * performs a HTML GET request to the given stop word list and forwards the data
     * to a list of strings.
     *
     * @return List of strings of the given stop words.
     * @throws IOException Throws Exception when IO operation is performed.
     */
    private static String[] requestAndSaveStopWords() throws IOException {

        URL urlString = new URL("https://www.textfixer.com/tutorials/common-english-words-with-contractions.txt");

        StringBuilder stopWords = new StringBuilder();

        // open the url stream with UTF-8 encoding.
        try (Scanner scanner = new Scanner(urlString.openStream(), StandardCharsets.UTF_8.toString())) {

            scanner.useDelimiter("\\A"); //RegEx pattern to begin at start of string.

            return stopWords.append(scanner.hasNext() ? scanner.next() : "")
                    .toString().split(","); //return string array of given stop words.
        }
    }

    /**
     * Gets stop words for filtering input data.
     *
     * @return List of stop words.
     */
    public static List<String> getStopWords() {
        try {
            return Arrays.asList(AnagramJobUtils.requestAndSaveStopWords());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Gets the command line arguments given from the driver method and generates
     * a integer array from the given values.
     *
     * @param cmdLineArgs Given command line arguments.
     * @return Integer array of sorting order values.
     */
    public static Integer[] getOptionals(String[] cmdLineArgs) {

        // Set the resultant optional command line results to a int array.
        // 1 is default value given, -1 is altered value.
        Integer[] resultArgs = new Integer[]{1, 1};

        // Process each argument and if it matches a sorting parameter
        // that isn't currently a default value.
        for (int i = 0; i < resultArgs.length; i++) {
            if (cmdLineArgs[i] != null && cmdLineArgs[i].equals("true")) {
                resultArgs[i] = -1;
            }
        }
        //Return altered parameter args.
        return resultArgs;
    }
}

