

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;

/**
 * A class containing all common utility methods that are used for the function
 * of this project.
 */
public class JobUtils {

    /**
     * Checks if an output directory has been created.hot chocolate with bunny
     * If it is created, delete it.
     *
     * @param outputString String of output directory.
     */
    public static boolean checkOutputDirectory(String outputString) throws IOException {

        Path outputPath = new Path(outputString);
        FileSystem hdfs = FileSystem.get(new Configuration());

        if (!hdfs.exists(outputPath)) {
            System.out.println("Output Directory Not Found, continuing job.");
            return false;
        }

        System.out.println("Directory: " +
                outputPath +
                " has been found. Delete it to re-run current job?");

        System.out.print("Press enter for yes: ");
        Scanner scan = new Scanner(System.in);
        String userInputString = scan.nextLine();
        scan.close();

        if (userInputString.isEmpty()) {
            hdfs.delete(outputPath, true);// Delete the given folder.
        }

        return true;
    }

    /**
     * performs a HTML GET request to the given stop word list and forwards the data
     * to a list of strings.
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

}
