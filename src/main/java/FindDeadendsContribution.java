import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FindDeadendsContribution {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        /**
         * input format: fromPage\t toPage1,toPage2,toPage3....
         * output format: fromPage\t ""
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] buffer = line.split("\t");

            if (buffer.length == 1 || buffer[1].trim().equals("")) { // bad data
                return;
            }

            String linkFrom = buffer[0];
            context.write(new Text(linkFrom),new Text(""));

        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        /**
         * input format: Page\t PageRank
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] pr = line.split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        double beta;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta",0.15f);
        }

        /**
         * if count > 1 means the key exists in both PrMatrix and transitionMatrix, which means it's not a dead end.
         * if count == 0 means the key doesn't exist in transitionMatrix, we need to sum up it's contribution.
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            Text v = null;
            for (Text value: values) {
                v = value;
                count++;
            }
            if (count > 1) return; // not a dead end
            if (count == 1) { // dead End
                context.write(new Text("DeadEndSum"),
                              new Text(String.valueOf(Double.parseDouble(v.toString())*(1-beta))));
            }

        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.setFloat("beta",Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);
        job.setJarByClass(FindDeadendsContribution.class);
        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
