import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mrdp.utils.MRDPUtils;

public class JoinTasksB extends Configured implements Tool {

	public String inputFilePost10000 = "/Users/arturofigueroa/Desktop/Maestria-DataScience/Lectures/2nd-Semester/Big-Data/assigments/a05/post10000.xml";
	public static String inputFileUser10000 = "/Users/arturofigueroa/Desktop/Maestria-DataScience/Lectures/2nd-Semester/Big-Data/assigments/a05/user10000.xml";
	public String outputFile = "/Users/arturofigueroa/JoinTasksB";
	String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());

	static java.util.Map<String, String> MRDPUPosts = new HashMap<String, String>();
	static java.util.Map<String, String> MRDPUUsers = new HashMap<String, String>();

	static HashMap<String, String> usersInfo = new HashMap<String, String>();

	static Set<String> userFileIds = new HashSet<String>();

	public static void readFiles(String route) {
		try {
			InputStream ips = new FileInputStream(route);
			InputStreamReader ipsr = new InputStreamReader(ips);
			BufferedReader br = new BufferedReader(ipsr);
			String line;

			while ((line = br.readLine()) != null) {
				MRDPUUsers = MRDPUtils.transformXmlToMap(line.toString());

				if (MRDPUUsers.get("Id") != null) {
					userFileIds.add(MRDPUUsers.get("Id"));
				}

			}

			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class JoinMapPost10000 extends Mapper<Object, Text, Text, Text> {
		private Text idUser = new Text();

		public JoinMapPost10000() {
			readFiles(inputFileUser10000);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			MRDPUPosts = MRDPUtils.transformXmlToMap(value.toString());

			if (MRDPUPosts.get("OwnerUserId") != null) {

				String id = MRDPUPosts.get("OwnerUserId");

				if (userFileIds.contains(id) && 10 <= Integer.parseInt(id) && Integer.parseInt(id) <= 15) {
					userFileIds.remove(id);
					idUser.set(id);
					context.write(idUser, null);
				}

			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "StackOverFlow Task");

		job.setJarByClass(JoinTasksB.class);
		job.setMapperClass(JoinMapPost10000.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputFilePost10000));
		FileOutputFormat.setOutputPath(job, new Path(outputFile + timeStamp));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new JoinTasksB(), args);
	}

}
