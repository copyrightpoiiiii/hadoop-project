package qi.hadoop.bayes;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class calcWordNumInData extends Configured implements Tool {

    public static class calcWordNumInDataMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

        private Text word = new Text();

        public void map(Text key, IntWritable value, Context context)
            throws IOException, InterruptedException {
                String[] line = key.toString().split("|");
                this.word.set(line[1]);
                context.write(this.word,new IntWritable(1));
            }
    }

    public static class calcWordNumInDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws  IOException, InterruptedException {
                context.write(key,new IntWritable(1));
            }
    }

    @Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Path outputPath = new Path("hdfs://master:9000/WordNumInData");
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		Job job = Job.getInstance(conf, "calcWordNumInData");
		
		job.setJarByClass(calcWordNumInData.class);
		job.setMapperClass(calcWordNumInDataMapper.class);
		job.setCombinerClass(calcWordNumInDataReducer.class);
		job.setReducerClass(calcWordNumInDataReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("hdfs://master:9000/WordInDoc"));
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new calcWordNumInData(), args);
		System.exit(res);
	}

}