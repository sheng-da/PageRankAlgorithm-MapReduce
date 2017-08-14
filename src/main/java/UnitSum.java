
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] PageRankUnit = value.toString().split("\t");
            double subPR = Double.parseDouble(PageRankUnit[1]);
            context.write(new Text(PageRankUnit[0]), new DoubleWritable(subPR));
        }
    }

    // add beta* PR (t-1) to result sum
    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        double beta;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           Configuration conf = context.getConfiguration();
           beta = conf.getFloat("beta", 0.15f);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pageRank = value.toString().split("\t");
            double betaRank = Double.parseDouble(pageRank[1])*beta;
            context.write(new Text(pageRank[0]), new DoubleWritable(betaRank));
        }
    }

    // sum up PR units and deadEnd units
    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable value: values) {
                sum += value.get();
            }

            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));

            context.write(key, new Text(Double.toString(sum)));
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.setFloat("beta",Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[4]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BetaMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

    }
}
