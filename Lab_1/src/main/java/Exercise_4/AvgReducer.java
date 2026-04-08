package Exercise_4;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AvgReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //0-18, 18-35, 35-50, 50+.
        double sum_0_18 = 0;
        int count_0_18 = 0;
        double sum_18_35 = 0;
        int count_18_35 = 0;
        double sum_35_50 = 0;
        int count_35_50 = 0;
        double sum_50_up = 0;
        int count_50_up = 0;
        for (Text v : values) {
            String[] parts = v.toString().split("\t");
            Integer age_value = Integer.parseInt(parts[1].trim());
            String rating_value = parts[3].trim();
            if (age_value >= 0 && age_value <=18) {
                sum_0_18 += Double.parseDouble(rating_value);
                count_0_18++;
            } else if (age_value >= 18 && age_value <= 35) {
                sum_18_35 += Double.parseDouble(rating_value);
                count_18_35++;
            } else if (age_value >= 35 && age_value <= 50) {
                sum_35_50 += Double.parseDouble(rating_value);
                count_35_50++;
            } else if (age_value >= 50) {
                sum_50_up += Double.parseDouble(rating_value);
                count_50_up++;
            }
        }
        String avg_0_18 = (count_0_18 > 0) ? String.format("%.2f", sum_0_18 / count_0_18) : "NA";
        String avg_18_35 = (count_18_35 > 0) ? String.format("%.2f", sum_18_35 / count_18_35) : "NA";
        String avg_35_50 = (count_35_50 > 0) ? String.format("%.2f", sum_35_50 / count_35_50) : "NA";
        String avg_50_up = (count_50_up > 0) ? String.format("%.2f", sum_50_up / count_50_up) : "NA";

        context.write(key, new Text(String.format("[0-18: %s, 18-35: %s, 35-50: %s, 50+: %s]", avg_0_18, avg_18_35, avg_35_50, avg_50_up)));
    }
}