import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

/**
 * DESCRIPTION: Using the Apache Hadoop Framework, it finds every common anagram
 * amongst a given dataset.
 * <p>
 * A more thorough description of the project can be found in the README markdown
 * document.
 */
public class AnagramCountJob extends Configured implements Tool {

    public static Integer[] optionalParamsValues;

    public static void main(String[] args) throws Exception {

        //Checks if output directory exists.
        if (AnagramJobUtils.checkOutputDirectory("output")) {
            System.out.println("Output directory deleted. continuing job.");
        }
        //Run MapReduce job.
        int res = ToolRunner.run(new Configuration(), new AnagramCountJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        //ANAGRAM HADOOP DRIVER CODE
        Configuration conf = this.getConf();

        optionalParamsValues = AnagramJobUtils.getOptionals(new String[]{
                conf.get("keyName.descending"),
                conf.get("frequency.descending")
        });

        Job job = Job.getInstance(conf, "Anagram Count");

        job.setJarByClass(AnagramCountJob.class);
        job.setMapperClass(AnagramMapper.class);
        job.setCombinerClass(AnagramCombiner.class);
        job.setReducerClass(AnagramReducer.class);
        job.setNumReduceTasks(1); //Produces only 1 output file.

        job.setOutputKeyClass(AnagramCompositeKey.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
