

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import java.net.URL;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
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
    public static String[] requestAndSaveStopWords() throws IOException {

        URL urlString = new URL("https://www.textfixer.com/tutorials/common-english-words-with-contractions.txt");

        StringBuilder stopWords = new StringBuilder();

        // open the url stream with UTF-8 encoding.
        try (Scanner scanner = new Scanner(urlString.openStream(), StandardCharsets.UTF_8.toString())) {

            scanner.useDelimiter("\\A"); //RegEx pattern to begin at start of string.

            return stopWords.append(scanner.hasNext() ? scanner.next() : "")
                    .toString().split(","); //return string array of given stop words.
        }
    }

    public static Boolean[] getOptionals(String[] cmdLineArgs) {

        // Set the resultant optional command line results to a boolean array.
        // False is default value given, true is altered value.
        Boolean[] resultArgs = new Boolean[]{false, false, false};

        if (cmdLineArgs.length <= 2) return resultArgs;
        // Checks if any optional parameters have been given. If not,
        // return the default values.

        for (String cmdLineArg : cmdLineArgs) {

            // Process each argument and if it matches a sorting parameter
            // that isn't currently a default value.
            switch (cmdLineArg) {

                case "sort=descending":
                    resultArgs[0] = true;
                    break;

                case "wordSort=descending":
                    resultArgs[1] = true;
                    break;

                case "setSort=descending":
                    resultArgs[2] = true;
                    break;

            }
        }
        //Return altered parameter args.
        return resultArgs;
    }

    public static Integer[] sortingBoolToInt(Boolean[] boolArray) {

        ArrayList<Integer> result = new ArrayList<>();
        //Initialise new mutable list

        for (boolean b : boolArray) {
            result.add(b ? -1 : 1);
        } // Iterate over booleans for conversion to
          // respective integer values.

       return result.toArray(new Integer[0]);
    }
}

