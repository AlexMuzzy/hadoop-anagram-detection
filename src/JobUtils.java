import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.Scanner;

/**
 * A class containing all common utility methods that are used for the function
 * of this project.
 */
public class JobUtils {

    /**
     * Checks if an output directory has been created.
     * If it is created, delete it.
     *
     * @param outputString String of output directory.
     */
    public static boolean checkOutputDirectory(String outputString){

        Path outputPath = Paths.get(outputString);


        if (!Files.exists(outputPath)) {
            System.out.println("Output Directory Not Found, continuing job.");
            return false;
        }

        System.out.println("Directory: " +
                outputPath +
                "has been found. Delete it to re-run current job?");

        System.out.print("Y for yes: ");
        Scanner scan = new Scanner(System.in);
        String userInputString = scan.next();
        scan.close();

        if (Objects.equals(userInputString, "Y")){
            //Grab all files in directory, then iteratively delete each file.
            //End with deleting folder.

            Arrays.stream(Objects.requireNonNull(
                    outputPath.toFile().list()))
                    .forEach(outputFile -> {
                        final boolean delete = new File(outputFile).delete();
                    });
        }

        return true;
    }



}
