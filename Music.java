package pk2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;




public class Music {
	
	public static void main(String [] args) throws Exception{
		Configuration c = new Configuration();
		
		String [] files = new GenericOptionsParser(c, args).getRemainingArgs();
		
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job j = new Job(c, "title");
		j.setJarByClass(Music.class);
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
			String line = value.toString();
			String [] lines = line.split("\n");
			for(String eachLine: lines){
				String []eachWord = eachLine.split(",");
				String trackid = eachWord[1];
				String shared = eachWord[2];
				int isshared = Integer.parseInt(shared);
				con.write(new Text(trackid), new IntWritable(isshared));
			}
		}
	}
		
		public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
			int maxi = 0;
			String trackid = "";
			public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
				int tot =0;
				for(IntWritable vals: values){
					tot += vals.get();
				}

				if(maxi < tot){
					maxi = tot;
					trackid = key.toString();
					
				}
				con.write(key, new IntWritable(tot));

			}
			
			@Override
			protected void cleanup(Context con) throws IOException, InterruptedException{
				con.write(new Text("max shared track : " + trackid), new IntWritable(maxi));
			}
		} // myreducer
	
	
	
}
