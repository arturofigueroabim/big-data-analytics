import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BlockingEntityResolution extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		private Text identifier = new Text();
		private Text name = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString()
					//.replaceAll("[0-9]", "").replace('-', ' ').replace('.', ' ').toLowerCase()
					.split(",");
			if (input.length > 2)
				return;

			String old_first_name_initial = input[0].trim().substring(0, 1);
			String[] old_name = input[0].trim().split(" ");
			String old_last_name = old_name[old_name.length - 1];

			identifier.set("s1_" + old_last_name);
			name.set(input[0].trim());
			context.write(identifier, name);

			identifier.set("s2_" + old_first_name_initial + old_last_name);
			name.set(input[0].trim());
			context.write(identifier, name);

			String new_first_name_initial = input[1].trim().substring(0, 1);
			String[] new_name = input[1].trim().split(" ");
			String new_last_name = new_name[new_name.length - 1];

			identifier.set("s1_" + new_last_name);
			name.set(input[1].trim());
			context.write(identifier, name);

			identifier.set("s2_" + new_first_name_initial + new_last_name);
			name.set(input[1].trim());
			context.write(identifier, name);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text value = new Text();
		

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> unique_names = new HashSet<String>();

			for (Text val : values) {
				unique_names.add(val.toString());
			}

			if (unique_names.contains("Douglas Schmidt")) {
				for (String unique_name : unique_names) {
					value.set(unique_name);
					context.write(key, value);
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "blocking entity resolution");
		job.setJarByClass(BlockingEntityResolution.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path output = new Path(args[1]);
		output.getFileSystem(conf).delete(output, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new BlockingEntityResolution(), args);
	}
}