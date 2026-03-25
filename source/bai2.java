import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class bai2 {
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieId = new Text();
        private Text outVal = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 3) return;

            movieId.set(parts[1].trim());
            outVal.set("R:" + parts[2].trim());

            context.write(movieId, outVal);
        }
    }
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieId = new Text();
        private Text outVal = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 3) return;

            movieId.set(parts[0].trim());
            outVal.set("G:" + parts[2].trim());

            context.write(movieId, outVal);
        }
    }
    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        Map<String, Double> sumMap = new HashMap<>();
        Map<String, Integer> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<Double> ratings = new ArrayList<>();
            String genres = "";

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("R:")) {
                    ratings.add(Double.parseDouble(v.substring(2)));
                } else if (v.startsWith("G:")) {
                    genres = v.substring(2);
                }
            }

            for (String g : genres.split("\\|")) {

                for (double r : ratings) {

                    sumMap.put(g, sumMap.getOrDefault(g, 0.0) + r);
                    countMap.put(g, countMap.getOrDefault(g, 0) + 1);
                }
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (String g : sumMap.keySet()) {

                double avg = sumMap.get(g) / countMap.get(g);

                context.write(
                        new Text(g),
                        new Text("Average rating: " + String.format("%.2f", avg)
                                + " (Total ratings: " + countMap.get(g) + ")")
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai2");

        job.setJarByClass(bai2.class);
        job.setReducerClass(GenreReducer.class);

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