import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * TITLE: AnagramCountJob.
 *
 * DESCRIPTION: Using the Apache Hadoop Framework, it finds every common anagram
 * amongst a given dataset.
 *
 * A more thorough description of the project can be found in the README markdown
 * document.
 *
 * All classes and methods within this project make use of JavaDocs comments.
 * JavaDocs Style guide used can be found here: https://blog.joda.org/2012/11/javadoc-coding-standards.html
 *
 * camelCase naming conventions are used in this project.
 */
public class AnagramCountJob {

    public static void main(String[] args) throws Exception {

        String outputPath = "output";

        if (JobUtils.checkOutputDirectory(outputPath)){
            System.out.println("Output directory deleted. continuing job.");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram Count");
        job.setJarByClass(AnagramCountJob.class);
        job.setMapperClass(AnagramMapper.class);
        job.setCombinerClass(AnagramReducer.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
