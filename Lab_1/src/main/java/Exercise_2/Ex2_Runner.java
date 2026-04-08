package Exercise_2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;
import org.apache.hadoop.io.DoubleWritable;
public class Ex2_Runner {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Ex2_Runner <movies> <rating_1> <rating_2> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Exercise 2 Average Rating by Genre (Map-side Join)");
        job.setJarByClass(Ex2_Runner.class);

        //Cache movies.txt file in Distributed Cache
        job.addCacheFile(new URI(args[0] + "#movies.txt"));

        job.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class); // rating_1
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class); // rating_2

        job.setReducerClass(AvgReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3])); // output
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}