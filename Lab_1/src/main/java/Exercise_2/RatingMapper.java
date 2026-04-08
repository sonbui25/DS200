package Exercise_2;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;

public class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final Map<Integer, String[]> movieGenres = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles= context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new IOException("Missing movies file in Distributed Cache");
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

                String genres = parts[2].trim();
                if (genres.isEmpty()) continue;

                String[] genreList = genres.split("\\|");
                movieGenres.put(movieID, genreList);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split(",");
        if (parts.length < 3) return;

        int movieID = Integer.parseInt(parts[1].trim());
        String rating = parts[2].trim();

        String[] genreList = movieGenres.get(movieID);
        if (genreList == null) return;

        for (String genre : genreList) {
            genre = genre.trim();
            if (genre.isEmpty()) continue;
            context.write(new Text(genre), new DoubleWritable(Double.parseDouble(rating)));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        movieGenres.clear();
    }
}