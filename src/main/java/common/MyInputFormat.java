package common;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


/**
 * Created by jianzchen on 2015/8/27.
 */
public class MyInputFormat extends FileInputFormat<LongWritable,Text>{
    public MyInputFormat(){
        super();
    }

    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,InterruptedException
    {
        return new MyRecordReader();
    }
}
