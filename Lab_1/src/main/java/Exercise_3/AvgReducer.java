package Exercise_3;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AvgReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum_M = 0;
        int count_M = 0;
        double sum_F = 0;
        int count_F = 0;
        for (Text v : values) {
            String[] parts = v.toString().split("\t");
            String gender_value = parts[1].trim();
            String rating_value = parts[3].trim();
            if (gender_value.equals("M")) {
                sum_M += Double.parseDouble(rating_value);
                count_M++;
            } else if (gender_value.equals("F")) {
                sum_F += Double.parseDouble(rating_value);
                count_F++;
            }
        }
        double avg_M = count_M > 0 ? sum_M / count_M : 0;
        double avg_F = count_F > 0 ? sum_F / count_F : 0;
        context.write(key, new Text(String.format("Male_Average_Rating: %.2f, Female_Average_Rating: %.2f", avg_M, avg_F)));
    }
}