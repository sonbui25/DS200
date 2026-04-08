package Exercise_2;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class AvgReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    @Override 
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            
            for (DoubleWritable v : values) {
                sum += v.get();
                count++;
            }
            double avg = count > 0 ? sum / count : 0;
            context.write(key, new Text(String.format("Average rating: %.2f (Total ratings: %d)", avg, count)));
    }
}