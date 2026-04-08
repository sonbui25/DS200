package Exercise_4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class Ex4_Runner {
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: Ex4_Runner <movies> <users> <rating_1> <rating_2> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Exercise 4 Average Rating by Age");
        job.setJarByClass(Ex4_Runner.class);

        //Cache movies.txt and users.txt file in Distributed Cache
        job.addCacheFile(new URI(args[0] + "#movies.txt"));
        job.addCacheFile(new URI(args[1] + "#users.txt"));

        job.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class); // rating_1
        MultipleInputs.addInputPath(job, new Path(args[3]),
        TextInputFormat.class, RatingMapper.class); // rating_2

        job.setReducerClass(AvgReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[4])); // output
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}