package job;

import common.MyRecordObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by cjz20 on 2015/12/21.
 */
public class RecordApp extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        int ret = ToolRunner.run(new RecordApp(), args);
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            String inputPath = args[0];
            String outputPath = args[1];
            //String processDate = PROCESSDATE.getValue();
            String reduceTask = args[2];

            Configuration configuration = new Configuration(getConf());
            //configuration.set("mapred.compress.map.output", "true");
            //configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
            if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0) {
                configuration.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
            }
            //configuration.set("processDate", processDate + "99");

            Job job = new Job(configuration, "RecordApp");
            job.setNumReduceTasks(Integer.parseInt(reduceTask));

            job.setJarByClass(Dedup.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            //job.setCombinerClass(Reduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            //FileOutputFormat.setCompressOutput(job, true);
            //FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);

            FileSystem fileSystem = FileSystem.get(configuration);
            if (fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            job.waitForCompletion(true);
        } catch (Exception exception) {
            exception.printStackTrace();
            return -1;
        }
        return 0;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private MyRecordObject myRecordObject;


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //String processDate = context.getConfiguration().get("processDate");
            try {
                myRecordObject = new MyRecordObject(value.toString());
                context.write(new Text(myRecordObject.getId().toString()), value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        Text outValue = new Text();
        private Iterator<Text> iterator;

        /*String outDate = "";
        Text tmpValue;
        String[] valueSplit;
        String tmpDate;
*/
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            iterator = values.iterator();

            while (iterator.hasNext()) {
/*                tmpValue = iterator.next();
                valueSplit = tmpValue.toString().split("\177");
                tmpDate = valueSplit[valueSplit.length - 1];
                if (tmpDate.compareTo(outDate) > 0) {
                    outValue = tmpValue;*/
                outValue = iterator.next();
            }

            try {
                context.write(new Text(), new Text(outValue));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

