
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org,apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class calcWordInDoc extends Configured implements Tool{


    public static class calcWordInDocMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {

        //匹配英文单词，(-[/sa-zA-Z])* 为了匹配人名等加有连字符的单词
        private static final Pattern PATTERN = Pattern.compile("[/sa-zA-Z]+(-[/sa-zA-Z])*");

        //record word
        private Text word = new Text();

        //meaningless words
        private static String[] meaningless = {"A", "a", "the", "an", "An", "in", "of", "from", "to", "on", "and", "The", "As", "as", "AND"}

        private static Vector<String> meaninglessWord(meaningless,meaningless+15)

        @Override
        public void map(Text key, BytesWritable value, Context context)
            throws IOException, InterruptedException {
            String line = new String(value.getBytes(),0,value.getLength());
            Matcher m = PATTERN.matcher(line);
            String category_file = key.toString().split("|");
            while(m.find()){
                String tword = m.group();
                if(!meaninglessWord.contains(tword)){
                    this.word.set(category_file[0] + "|" + tword);
                    context.write(this.word,new IntWritable(1));
                }
            }
        }
        
    }

    public static class calcWordInDocReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable wordSum = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable) value, Context context)
            throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable item : value){
                    sum += item.get();
                }
                this.wordSum.set(sum);
                context.write(key,this.wordSum);
            }
    }   

    @Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
		}

		Path outputPath = new Path("hdfs://master:9000/WordInDoc");
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job job = Job.getInstance(conf, "calcWordInDoc");

		job.setJarByClass(calcWordInDoc.class);
		job.setMapperClass(calcWordInDocMapper.class);
		job.setCombinerClass(calcWordInDocReducer.class);
		job.setReducerClass(calcWordInDocReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("hdfs://master:9000/SerializationTrainData"));
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new calcWordInDoc(), args);
		System.exit(res);
	}

}
