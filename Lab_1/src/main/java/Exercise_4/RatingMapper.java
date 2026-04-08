package Exercise_4;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Map<Integer, String> movieTitles = new HashMap<>();
    private final Map<Integer, Integer> userAges = new HashMap<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new IOException("Missing movies and users files in Distributed Cache");
        }    

        // Xét cacheFiles[0] là file movies.txt
        try (BufferedReader reader = new BufferedReader(new FileReader("./movies.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",", 3);
                if (parts.length < 3) continue;

                int movieID = Integer.parseInt(parts[0].trim());
                String title = parts[1].trim();
                movieTitles.put(movieID, title);
            }
        }

        // Xét cacheFiles[1] là file users.txt
        try (BufferedReader reader = new BufferedReader(new FileReader("./users.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",", 5);
                if (parts.length < 5) continue;

                int userID = Integer.parseInt(parts[0].trim());
                Integer age = Integer.parseInt(parts[2].trim());
                userAges.put(userID, age);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split(",", 4);
        if (parts.length < 4) return;

        int userID = Integer.parseInt(parts[0].trim());
        int movieID = Integer.parseInt(parts[1].trim());
        String rating = parts[2].trim();
        Integer age = userAges.get(userID);
        String title = movieTitles.get(movieID);

        if (age == null || title == null) return;

        context.write(new Text(title), new Text(String.format("A\t%s\tR\t%s", age, rating)));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        userAges.clear();
        movieTitles.clear();
    }
}