package Exercise_1;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split(",");
        if (parts.length < 3) return;

        int movieID = Integer.parseInt(parts[1].trim());
        String rating = parts[2].trim();

        // Tag R để reducer biết đây là bản ghi từ rating
        context.write(new IntWritable(movieID), new Text("R\t" + rating));
    }
}
