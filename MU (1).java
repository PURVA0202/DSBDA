package Music;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;


public class MU {
	public static void main(String []args) throws Exception{
		Configuration c = new Configuration();
		String []files = new GenericOptionsParser(c, args).getRemainingArgs();
		
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job j = new Job(c, "music dataset");
		j.setJarByClass(MU.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String tmp = value.toString();
			String [] lines = tmp.split("\n");
			for (String line: lines){
				String[] eachLine = line.split(",");
				String trackId = eachLine[1];
				String isShared = eachLine[2];
				if(Integer.parseInt(isShared) != 0){
					con.write(new Text(trackId), new IntWritable(1));
				}
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable>values, Context con) throws IOException, InterruptedException{
			int count = 0;
			for(IntWritable val: values){
				count += val.get();
			}
			con.write(key, new IntWritable(count));
		}
		
	}

}
