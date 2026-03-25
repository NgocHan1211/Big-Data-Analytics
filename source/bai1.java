import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class bai1 {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 3) return;

            try {
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                movieIdKey.set(movieId);
                ratingValue.set("Rate: " + rating);

                context.write(movieIdKey, ratingValue);

            } catch (Exception e) {}
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieIdKey = new Text();
        private Text movieValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 2) return;

            String movieId = parts[0].trim();
            String movieName = parts[1].trim();

            movieIdKey.set(movieId);
            movieValue.set("Movie: " + movieName);

            context.write(movieIdKey, movieValue);
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {

        double maxRating = 0;
        String maxMovie = "";

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            String movieName = "";

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("Rate:")) {
                    sum += Double.parseDouble(v.replace("Rate:", "").trim());
                    count++;
                } else if (v.startsWith("Movie:")) {
                    movieName = v.replace("Movie:", "").trim();
                }
            }

            if (count > 0) {
                double avg = sum / count;

                context.write(
                        new Text(movieName),
                        new Text("Average rating: " + String.format("%.2f", avg)
                                + " (Total: " + count + ")")
                );

                if (count >= 5 && avg > maxRating) {
                    maxRating = avg;
                    maxMovie = movieName;
                }
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            if (!maxMovie.equals("")) {
                context.write(new Text("RESULT"),
                        new Text(maxMovie + " is highest with " + String.format("%.2f", maxRating)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai1");

        job.setJarByClass(bai1.class);
        job.setReducerClass(RatingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]),
                TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}