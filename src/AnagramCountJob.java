import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * DESCRIPTION: Using the Apache Hadoop Framework, it finds every common anagram
 * amongst a given dataset.
 * <p>
 * A more thorough description of the project can be found in the README markdown
 * document.
 * <p>
 * All classes and methods within this project make use of JavaDocs comments.
 * JavaDocs Style guide used can be found here: https://blog.joda.org/2012/11/javadoc-coding-standards.html
 * <p>
 * camelCase naming conventions are used in this project.
 */
public class AnagramCountJob {

    public static String[] cmdLineArgs;
    public static Integer[] paramArgs;

    public static void main(String[] args) throws Exception {

        cmdLineArgs = args;
        //initial command line parameters.

        paramArgs = AnagramJobUtils.getOptionals(cmdLineArgs);
        //Command line parameters which have been processed

        //Checks if output directory exists.
        if (AnagramJobUtils.checkOutputDirectory("output")) {
            System.out.println("Output directory deleted. continuing job.");
        }

        //ANAGRAM HADOOP DRIVER CODE
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram Count");

        job.setJarByClass(AnagramCountJob.class);
        job.setMapperClass(AnagramMapper.class);
        job.setCombinerClass(AnagramCombiner.class);
        job.setReducerClass(AnagramReducer.class);

        job.setOutputKeyClass(AnagramCompositeKey.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(cmdLineArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(cmdLineArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
