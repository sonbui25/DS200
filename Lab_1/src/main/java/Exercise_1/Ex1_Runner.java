import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
public class Ex1_Runner {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Ex1_Runner <movies> <rating_1> <rating_2> <output>");
            System.exit(1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Exercise 1 Average Rating");
    job.setJarByClass(Ex1_Runner.class);
    
    job.setReducerClass(AvgReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class); // movies
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class); // rating_1
    MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class); // rating_2

    FileOutputFormat.setOutputPath(job, new Path(args[3])); // output
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}