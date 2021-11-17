
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org,apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class calcWordNumInDoc extends Configured implements Tool {

    public static class calcWordNumInDocMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

        private Text category = new Text();

        public void map(Text key, IntWritable value, Context context)
            throws IOException, InterruptedException {
                String[] line = key.toString().split("|");
                this.category.set(line[0]);
                context.write(this.category,value);
            }
    }

    public static class calcWordNumInDocReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable wordSum = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws  IOException, InterruptedException {
                int sum =0;
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
		
		Path outputPath = new Path("hdfs://master:9000/WordNumInDoc");
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		Job job = Job.getInstance(conf, "calcWordNumInDoc");
		
		job.setJarByClass(calcWordNumInDoc.class);
		job.setMapperClass(calcWordNumInDocMapper.class);
		job.setCombinerClass(calcWordNumInDocReducer.class);
		job.setReducerClass(calcWordNumInDocReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("hdfs://master:9000/WordInDoc"));
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new calcWordNumInDoc(), args);
		System.exit(res);
	}
}