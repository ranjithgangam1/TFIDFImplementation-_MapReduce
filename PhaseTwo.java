import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PhaseTwo {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String inputLine = value.toString();
			String temp[] = inputLine.split("\t"); // spliting input string to
			int wordCntr = Integer.parseInt(temp[1]);// getting word frequency
			String docPart[] = temp[0].split(",");// seperating document name
			String docName = docPart[1]; // getting the document number or the
			outKey.set(docName);
			String tempStr = "";
			tempStr = docPart[0];
			tempStr = tempStr + "," + wordCntr;
			outValue.set(tempStr);

			context.write(outKey, outValue);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text outValue1 = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int N = 0;

			ArrayList<String> AllValues = new ArrayList<String>();
			HashMap<String, String> Map_allValues = new HashMap<String, String>();
			//Map_allValues.put(key,(Text)values);

			for (Text text : values) {
				String content[] = text.toString().split(",");
				Map_allValues.put(key +","+ content[0], content[1]);
				N = N + Integer.parseInt(content[1]);

			}
			ArrayList<Float> tf_arr = new ArrayList<Float>();

			Iterator<Entry<String, String>> itr = Map_allValues.entrySet()
					.iterator();
			while (itr.hasNext()){
				String key1 = itr.next().getKey();
				String value = Map_allValues.get(key1);
				//String content[] = value.toString().split(",");
				float tf = Float.parseFloat(value.toString()) / N;
				String a;
				a = value.toString() + "," + Float.toString(tf);
				Map_allValues.put(key1, a);
				tf_arr.add(tf);
			}

			float max = Collections.max(tf_arr);

			/*for (String text : AllValues) {
				String content[] = text.toString().split(",");
				tf_arr.add(Float.parseFloat(content[1]) / N);
			}*/

			Iterator<Entry<String, String>> itr1 = Map_allValues.entrySet()
					.iterator();
			while (itr1.hasNext()) {
				String key1 = itr1.next().getKey();
				// System.out.println(itr.next());
				String value = Map_allValues.get(key1);
				String content[] = value.toString().split(",");
				float tf = Float.parseFloat(content[1]) / max;
				String a;
				a = value.toString() + "," + Float.toString(tf);
				//value.set(a);
				//String newkey = key.toString() + "," + content[0];
				context.write(new Text(key1), new Text(a));
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PhaseOne");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(PhaseTwo.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
	}

}
