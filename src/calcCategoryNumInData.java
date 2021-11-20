package qi.hadoop.bayes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class calcCategoryNumInData extends Configured implements Tool {

    public static class calcCategoryNumInDataMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {

        private Text category = new Text();

        public void map(Text key, BytesWritable value, Context context)
            throws IOException, InterruptedException {
                String[] line = key.toString().split("|");
                this.category.set(line[0]);
                context.write(this.category,new IntWritable(1));
            }
    }

    public static class calcCategoryNumInDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text category = new Text();

        public void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws  IOException, InterruptedException {
                int sum = 0;
                for(IntWritable item : value){
                    sum += item.get();
                }
                this.category.set(key);
                context.write(this.category,new IntWritable(sum));
            }
    }

    @Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Path outputPath = new Path("hdfs://master:9000/CategoryNum");
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		Job job = Job.getInstance(conf, "calcCategoryNumInData");
		
		job.setJarByClass(calcCategoryNumInData.class);
		job.setMapperClass(calcCategoryNumInDataMapper.class);
		job.setCombinerClass(calcCategoryNumInDataReducer.class);
		job.setReducerClass(calcCategoryNumInDataReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("hdfs://master:9000/SerializationTrainData"));
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new calcCategoryNumInData(), args);
		System.exit(res);
	}
}