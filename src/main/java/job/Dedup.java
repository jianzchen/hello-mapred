package job;

//import com.ebay.hadoop.platform.common.OptionsHelper;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Description: transform the text file into FormattedSequenceFile and dedup based on LAST_MODIFIED_DATE (last field)
 * Author: Johnson CHEN
 * Date: 2014/7/7.
 */
public class Dedup extends Configured implements Tool {

    /*    protected static final Option INPUT = OptionBuilder.hasArg().isRequired(true).create("in");
        protected static final Option OUTPUT = OptionBuilder.hasArg().isRequired(true).create("out");
        //protected static final Option PROCESSDATE = OptionBuilder.hasArg().isRequired(true).create("process_date");
        protected static final Option REDUCETASK = OptionBuilder.hasArg().isRequired(true).create("reduce_task");

        private Options options = new Options();
        //private OptionsHelper optionsHelper = new OptionsHelper();
    */
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Dedup(), args);
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
/*        try {
            this.options.addOption(this.INPUT);
            this.options.addOption(this.OUTPUT);
            //this.options.addOption(this.PROCESSDATE);
            this.options.addOption(this.REDUCETASK);
            //this.optionsHelper.parseOptions(this.options, args);
        } catch (Exception exception) {
            //this.optionsHelper.printUsage(getClass().getSimpleName(), this.options);
            exception.printStackTrace();
            return -1;
        }*/
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

            Job job = new Job(configuration, "Flat File Dedup");
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
            job.setOutputFormatClass(TextOutputFormat.class);

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
        private String[] split;


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //String processDate = context.getConfiguration().get("processDate");
            try {
                split = value.toString().split(",", 2);
                context.write(new Text(split[0]), value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Iterator<Text> iterator;
        Text outValue = new Text();
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