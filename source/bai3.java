import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai3 {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 3) return;

            try {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                context.write(new Text(userId),
                        new Text("R:" + movieId + ":" + rating));

            } catch (Exception e) {}
        }
    }

    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 2) return;

            String userId = parts[0].trim();
            String gender = parts[1].trim();

            context.write(new Text(userId),
                    new Text("U:" + gender));
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {

        Map<String, String> movieMap = new HashMap<>();
        Map<String, List<Double>> resultMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {

            URI[] files = context.getCacheFiles();

            if (files == null) return;

            for (URI file : files) {

                try {
                    String fileName = new Path(file.getPath()).getName();

                    BufferedReader br = new BufferedReader(
                            new FileReader(new File(fileName))
                    );

                    String line;

                    while ((line = br.readLine()) != null) {

                        String[] parts = line.split(",");
                        if (parts.length >= 2) {
                            movieMap.put(parts[0].trim(), parts[1].trim());
                        }
                    }

                    br.close();

                } catch (Exception e) {
                    // tránh crash 
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String gender = "";
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("U:")) {
                    gender = v.substring(2);
                } else if (v.startsWith("R:")) {
                    ratings.add(v.substring(2));
                }
            }

            if (gender.equals("")) return;

            for (String r : ratings) {

                String[] parts = r.split(":");
                if (parts.length < 2) continue;

                try {
                    String movieId = parts[0];
                    double rating = Double.parseDouble(parts[1]);

                    String keyMG = movieId + "_" + gender;

                    resultMap.putIfAbsent(keyMG, new ArrayList<>());
                    resultMap.get(keyMG).add(rating);

                } catch (Exception e) {}
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            Map<String, Map<String, Double>> finalMap = new HashMap<>();

            for (String key : resultMap.keySet()) {

                String[] parts = key.split("_");
                if (parts.length < 2) continue;

                String movieId = parts[0];
                String gender = parts[1];

                List<Double> list = resultMap.get(key);
                if (list == null || list.isEmpty()) continue;

                double sum = 0;
                for (double r : list) sum += r;

                double avg = sum / list.size();

                finalMap.putIfAbsent(movieId, new HashMap<>());
                finalMap.get(movieId).put(gender, avg);
            }

            for (String movieId : finalMap.keySet()) {

                String title = movieMap.getOrDefault(movieId, "Unknown");

                double male = finalMap.get(movieId).getOrDefault("M", 0.0);
                double female = finalMap.get(movieId).getOrDefault("F", 0.0);

                context.write(
                        new Text(title),
                        new Text("Male: " + String.format("%.2f", male) +
                                ", Female: " + String.format("%.2f", female))
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: bai3 <ratings1> <ratings2> <users> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Analysis");

        job.setJarByClass(bai3.class);

        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[2]),
                TextInputFormat.class, UserMapper.class);

        job.addCacheFile(new Path("/user/ngochan1211/input/movies.txt").toUri());

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}