import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.text.DecimalFormat;

public class ApplyDeadendSum {
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private static int WEBSITE_COUNT;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            WEBSITE_COUNT = Integer.parseInt(conf.get("websitesCount"));
        }

        // find the dead ends contributions and apply the total to all pages
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] pageRank = value.toString().split("\t");
            double buff = Double.parseDouble(pageRank[1]);

            if (pageRank[0].equals("DeadEndSum")) {
                for (int i = 1 ; i <= WEBSITE_COUNT;i++) {
                    context.write(new Text(Integer.toString(i)),new DoubleWritable(buff/WEBSITE_COUNT));
                }
            } else {
                context.write(new Text(pageRank[0]), new DoubleWritable(buff));
            }
        }
    }


    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable value: values) {
                sum += value.get();
            }

            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));

            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.set("websitesCount",args[2]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ApplyDeadendSum.class);
        job.setMapperClass(PassMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}
