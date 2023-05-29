package LogFile;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;



public class WC {
	public static void main(String [] args) throws Exception{
		Configuration c = new Configuration();
		String [] files = new GenericOptionsParser(c, args).getRemainingArgs();
		
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job j = new Job(c, "log file");
		j.setJarByClass(WC.class);
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String lines = value.toString();
			String[] line = lines.split("\n");
			for(String word: line){
				String [] eachLine = word.split(",");
				String [] eachWord = eachLine[5].split(" ");
				String[] eachWord2 = eachLine[7].split(" ");
				
				String startTime = eachWord[1];
				String endTime = eachWord2[1];
				
				int h1 = Integer.parseInt(startTime.substring(0,2));
				int m1 = Integer.parseInt(startTime.substring(3, 5));
				int s1 = Integer.parseInt(startTime.substring(6, 8));
				
				int h2 = Integer.parseInt(endTime.substring(0, 2));
				int m2 = Integer.parseInt(endTime.substring(3, 5));
				int s2 = Integer.parseInt(endTime.substring(6, 8));
				
				int totsec1 = h1 * 3600 + m1 * 60 + s1;
				int totsec2 = h2 * 3600 + m2 * 60 + s2;
				int diff = totsec2 - totsec1;
				
				int ansHour = diff/3600;
				int ansMin = (diff % 3600)/60;
				int ansSec = (diff % 3600) % 60;
				String ans = ansHour + ":" + ansMin + ":" + ansSec + " by " + eachLine[1];
				Text out = new Text(ans);
				con.write(out, new IntWritable(1));
				
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text word, Iterable<IntWritable>values, Context con) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val: values){
				sum += val.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}
