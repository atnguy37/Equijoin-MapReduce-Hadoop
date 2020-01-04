// package dds.assignment4;
import java.io.IOException;

import java.util.List;
// import java.util.Arrays;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	
public class equijoin
{
	
	// class : Mapper - mapping phase.
	public static class RatingMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			// System.out.println("Map Phase");
            String mapValue = value.toString().trim();

			// mapperkey parsing which would result in the keys of the set.

			Long mapKey = Long.parseLong(mapValue.split(", ")[1]); 

			// System.out.println("Key: " + mapKey);	// for reference 
			// System.out.println("Value: " + mapValue);	//for reference 
			// String out = String.format()
			context.write(new LongWritable(mapKey), new Text("rating\t" + mapValue)); // writing the key,value pairs
		}
	}


	public static class MovieMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			// System.out.println("Map Phase");
            String mapValue = value.toString().trim();

			// mapperkey parsing which would result in the keys of the set.

			Long mapKey = Long.parseLong(mapValue.split(", ")[0]); 

			// System.out.println("Key: " + mapKey);	// for reference 
			// System.out.println("Value: " + mapValue);	//for reference 
			
			context.write(new LongWritable(mapKey), new Text("movie\t" +mapValue)); // writing the key,value pairs
		}
	}
	
																																				

	// class : Reducer - reduce phase.
	public static class ReducerClass extends Reducer<LongWritable, Text, Object, Text> 
	{	
	   protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	   {
		   	// System.out.println("Reduce Phase");
			// String table1 = null;
			String movieKey = null;
		   	List<String> items = new ArrayList<String>();
			List<String> ratingItems = new ArrayList<String>();
		   
		   // looping the list for text values.
		   
		   for (Text value : values) 
			{   
                // System.out.println("Value: " + value);
				if (value.toString().split("\t")[0].equals("movie"))
					movieKey = value.toString().split("\t")[1];
				else 
					ratingItems.add(value.toString().split("\t")[1]);
			
			}
			if (movieKey == null)
				return;
		//    System.out.println("table1Items: " + String.join(": ", table1Items));
		//   System.out.println("table2Items: " + String.join(": ", table2Items));
		   
			StringBuilder outputString = new StringBuilder();
			for (String item : ratingItems) {
				outputString.append(item).append(", ").append(movieKey).append("\n");	
			}
			System.out.println("Result: " + String.join(": ", outputString));
			context.write(null, new Text(outputString.toString().trim()));

		   
		}
	}
	

	public static void main(String[] args) throws Exception 
	{
	    Configuration config = new Configuration(); // job assignment config
	    Job job = Job.getInstance(config, "equijoin"); // job config with class

		job.setJarByClass(equijoin.class);
	    // job.setMapperClass(RatingMapper.class);
	    job.setReducerClass(ReducerClass.class);	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Object.class);
		job.setOutputValueClass(Text.class);


		


		MultipleInputs.addInputPath(job, new Path(args[0]),
		TextInputFormat.class, RatingMapper.class);

		MultipleInputs.addInputPath(job, new Path(args[1]),
		TextInputFormat.class, MovieMapper.class);

		System.out.println("Arg: " + String.join(": ",args));
	    // formatting.

	    // FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
	    // conditional if to exit
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
}