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
import java.util.LinkedList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        /**
         * input format: fromPage\t toPage1,toPage2,toPage3....
         * output format: fromPage\t toPage=probability
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            String line = value.toString().trim();
            String[] buffer = line.split("\t");

            if (buffer.length == 1 || buffer[1].trim().equals("")) { // bad data
                return;
            }
            String linkFrom = buffer[0];
            String[] linkTo = buffer[1].split(",");
            int linkCount = linkTo.length;

            for (String s: linkTo) {
                context.write(new Text(linkFrom),new Text(s + "=" + (double)1/linkCount));
            }
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


        float beta;  //"damping factor"

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta",0.15f);
        }

        /**
         * input format:
         * key = fromPage value = toPage=probability
         *     or
         * key = fromPage value = pageRank
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            List<String> transitionUnit = new LinkedList<String>();
            double prValue = 0;
            for (Text value: values) {
                if (value.toString().contains("=")) {
                    transitionUnit.add(value.toString());
                } else {
                    prValue = Double.parseDouble(value.toString());
                }
            }

            for (String unit: transitionUnit) {
                String[] buff = unit.split("=");
                String toLink = buff[0];
                Double probability = Double.parseDouble(buff[1]);
                String prUnit = String.valueOf(prValue * probability*(1-beta));
                context.write(new Text(toLink), new Text(prUnit));
            }
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.setFloat("beta",Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);
        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
