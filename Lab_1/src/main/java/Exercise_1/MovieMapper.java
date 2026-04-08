package Exercise_1;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split(",", 3);
        if (parts.length < 3) return;

        int movieID = Integer.parseInt(parts[0].trim());
        String title = parts[1].trim();

        context.write(new IntWritable(movieID), new Text("M\t" + title));
    }
}
