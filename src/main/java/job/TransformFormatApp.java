package job;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/*
import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.example.ExampleInputFormat;
*/

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Created by chenjianzhou622 on 2016/1/8.
 */
public class TransformFormatApp extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new TransformFormatApp(), args);
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

            Configuration configuration = new Configuration(getConf());
            //configuration.set("mapred.compress.map.output", "true");
            //configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
            if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0) {
                configuration.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
            }
            //configuration.set("processDate", processDate + "99");

            Job job = new Job(configuration, "Transform Format");

            job.setJarByClass(Dedup.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            //job.setCombinerClass(Reduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));

            job.setOutputFormatClass(ParquetOutputFormat.class);

            FileSystem fileSystem = FileSystem.get(configuration);
            if (fileSystem.exists(new Path("data/out"))) {
                fileSystem.delete(new Path("data/out"), true);
            }

            String writeSchema = "message example {\n" +
                    "required binary x (UTF8);\n" +
                    "required binary y (UTF8);\n" +
                    "required binary z (UTF8);\n" +
                    "}";
            MessageType schema = MessageTypeParser.parseMessageType(writeSchema);
            GroupWriteSupport writeSupport = new GroupWriteSupport();
            GroupWriteSupport.setSchema(schema, configuration);
            GroupFactory groupFactory = new SimpleGroupFactory(schema);
            Group group = groupFactory.newGroup()
                    .append("x", "X")
                    .append("y", "Y")
                    .append("z","Z");
            ParquetWriter<Group> writer = new ParquetWriter<Group>(new Path(outputPath), writeSupport,
                    ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE, /* dictionary page size */
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetProperties.WriterVersion.PARQUET_1_0, configuration);
            writer.write(group);
            writer.close();
/*            ParquetOutputFormat.setWriteSupportClass(
                    job,
                    MessageTypeParser.parseMessageType(writeSchema).getClass()
                    );
            ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);

            FileOutputFormat.setOutputPath(job, new Path(outputPath));*/

            //FileOutputFormat.setCompressOutput(job, true);
            //FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);


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
