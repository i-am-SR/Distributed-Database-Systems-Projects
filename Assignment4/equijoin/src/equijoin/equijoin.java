package equijoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class equijoin 
{
	public static class MyMapper extends Mapper<Object, Text, DoubleWritable, Text> 
	{
		private DoubleWritable key = new DoubleWritable();
		private Text value = new Text();
		public void map(Object mapKey, Text mapValue, Context mapContext) throws IOException, InterruptedException {
			String []entries = mapValue.toString().split("\\r?\\n");	// Split the input on "\n" to get different rows
			for (String entry: entries) 
			{
				String []values = entry.split(", ");
				key.set(Double.parseDouble(values[1]));
				value.set(entry);
				mapContext.write(key, value);	//Output of the Mapper
			}
		}
	}

	public static class MyReducer extends Reducer<DoubleWritable, Text, Object, Text> 
	{
		private Text value = new Text();
		public void reduce(DoubleWritable reduceKey, Iterable<Text> reduceValues, Context reduceContext) throws IOException, InterruptedException 
		{
			List<String> entries = new ArrayList<>();			
			StringBuilder output = new StringBuilder();
			for(Text entry: reduceValues)
				entries.add(entry.toString());
			for(int i1 = 0; i1 < entries.size() - 1; i1++)
			{
				for(int i2 = i1 + 1 ; i2 < entries.size(); i2++) 	// This nested loop avoids duplicate join results.
				{
					String entry1 = entries.get(i1);
					String entry2 = entries.get(i2);
					if(!entry2.split(", ")[0].equals(entry1.split(", ")[0]))	//If the two rows are not from the same table
					{
					output.append(entry1);
					output.append(", ");
					output.append(entry2);
					output.append("\n");
					}
				}
			}
			if(output.length() != 0)
			{
				value.set(output.toString().trim());
				reduceContext.write(null, value);	//Output of the Reducer
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration config = new Configuration();
		Job join = Job.getInstance(config, "equijoin");
		join.setJarByClass(equijoin.class);
		join.setMapperClass(MyMapper.class);
		join.setReducerClass(MyReducer.class);
		join.setMapOutputKeyClass(DoubleWritable.class);
		join.setMapOutputValueClass(Text.class);
		join.setOutputKeyClass(Object.class);
		join.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(join, new Path(args[1]));
		FileOutputFormat.setOutputPath(join, new Path(args[2]));
		System.exit(join.waitForCompletion(true) ? 0 : 1);	//Exit after completetion
	}
}