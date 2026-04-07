import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReducer extends Reducer<IntWritable, Text, Text, Text> {
    private String maxMovie = null;
    private double maxRating = -1;
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            String movieTitle = "";
            for (Text v : values) {
                String tag = v.toString().split("\t")[0].trim();
                String value = v.toString().split("\t")[1].trim();
                if (tag.equals("R")){
                    sum += Double.parseDouble(value);
                    count++;
                }
                else if (tag.equals("M")) {
                    movieTitle = value;
                }
            }
            double avg = count > 0 ? sum / count : 0;
            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieTitle;
            }   
            context.write(new Text(movieTitle), new Text(String.format("Average rating: %.2f (Total ratings: %d)", avg, count)));    
    }
    
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
            if (maxRating > 0) {
                context.write(new Text(maxMovie), new Text(String.format("is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.", maxRating)));
            } else {
                context.write(new Text("\nNo movie with at least 5 ratings found."), new Text(""));
            }
    }
}
