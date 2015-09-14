package common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by jianzchen on 2015/8/27.
 */
public class MyRecordReader extends RecordReader<LongWritable,Text> {

    private static final Log LOG = LogFactory.getLog(MyRecordReader.class);
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long end;
    private long pos;
    private FSDataInputStream fin = null;
    private LongWritable key = null;
    private Text value = null;
    private MyLineReader reader = null;
    private byte[] lineDelimeter = "\000\000\000\000\n".getBytes();

    public void close() throws IOException {
        fin.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
// TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        Configuration conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(conf);
        fin = fs.open(path);
        fin.seek(start);
        reader = new MyLineReader(fin);
        pos = 1;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null)
            key = new LongWritable();
        key.set(pos);
        if (value == null)
            value = new Text();
        if (reader.readLine(value) == 0)
            return false;
        pos++;
        return true;
    }

    private class MyLineReader {
        private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private InputStream in;
        private byte[] buffer;
        private int bufferLength = 0;
        private int bufferPosn = 0;
        public MyLineReader(InputStream in) {
            this(in, DEFAULT_BUFFER_SIZE);
        }
        public MyLineReader(InputStream in, int bufferSize) {
            this.in = in;
            this.bufferSize = bufferSize;
            this.buffer = new byte[this.bufferSize];
        }

        public MyLineReader(InputStream in, Configuration conf)
                throws IOException {
            this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
        }

        public void close() throws IOException {
            in.close();
        }

        public int readLine(Text str, int maxLineLength, int maxBytesToConsume)

                throws IOException {
            str.clear();
            Text record = new Text();
            int txtLength = 0;
            long bytesConsumed = 0L;
            boolean newline = false;
            int sepPosn = 0;
            do {
                // 已经读到buffer的末尾了，读下一个buffer
                if (this.bufferPosn >= this.bufferLength) {
                    bufferPosn = 0;
                    bufferLength = in.read(buffer);
                    // 读到文件末尾了，则跳出，进行下一个文件的读取
                    if (bufferLength <= 0) {
                        break;
                    }
                }
                int startPosn = this.bufferPosn;
                for (; bufferPosn < bufferLength; bufferPosn++) {
                    // 处理上一个buffer的尾巴被切成了两半的分隔符(如果分隔符中重复字符过多在这里会有问题)
                    if (sepPosn > 0 && buffer[bufferPosn] != lineDelimeter[sepPosn]) {
                        sepPosn = 0;
                    }

                    // 遇到行分隔符的第一个字符
                    if (buffer[bufferPosn] == lineDelimeter[sepPosn]) {
                        bufferPosn++;
                        int i = 0;
                        // 判断接下来的字符是否也是行分隔符中的字符
                        for (++sepPosn; sepPosn < lineDelimeter.length; i++, sepPosn++) {
                            // buffer的最后刚好是分隔符，且分隔符被不幸地切成了两半
                            if (bufferPosn + i >= bufferLength) {
                                bufferPosn += i - 1;
                                break;
                            }
                            // 一旦其中有一个字符不相同，就判定为不是分隔符
                            if (this.buffer[this.bufferPosn + i] != lineDelimeter[sepPosn]) {
                                sepPosn = 0;
                                break;
                            }
                        }

                        // 的确遇到了行分隔符
                        if (sepPosn == lineDelimeter.length) {
                            bufferPosn += i;
                            newline = true;
                            sepPosn = 0;
                            break;
                        }
                    }
                }

                int readLength = this.bufferPosn - startPosn;
                bytesConsumed += readLength;
                // 行分隔符不放入块中
                if (readLength > maxLineLength - txtLength) {
                    readLength = maxLineLength - txtLength;
                }

                if (readLength > 0) {
                    record.append(this.buffer, startPosn, readLength);
                    txtLength += readLength;
                    // 去掉记录的分隔符
                    if (newline) {
                        str.set(record.getBytes(), 0, record.getLength()
                                - lineDelimeter.length);
                    }
                }

            } while (!newline && (bytesConsumed < maxBytesToConsume));
            if (bytesConsumed > (long) Integer.MAX_VALUE) {
                throw new IOException("Too many bytes before newline: "
                        + bytesConsumed);
            }
            return (int) bytesConsumed;
        }

        public int readLine(Text str, int maxLineLength) throws IOException {
            return readLine(str, maxLineLength, Integer.MAX_VALUE);
        }

        public int readLine(Text str) throws IOException {
            return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
        }
    }

}