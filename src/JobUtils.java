

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
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

        if (userInputString.isEmpty()){
            hdfs.delete(outputPath, true);// Delete the given folder.
        }

        return true;
    }



}
