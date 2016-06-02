import java.io.IOException;
import java.util.*;
import java.math.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
        
public class PhaseThree 
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
	private final static IntWritable one = new IntWritable(1);
	private Text outKey = new Text();
	private Text outValue = new Text();
        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	    String inputLine = value.toString(); //input is coming from the output file from phase one
	    String temp[] = inputLine.split("\t"); //spliting input string to get pair of word,document name and frequency
	     String docPart[]=temp[1].split(",");//seperating document name and word
	    String docnames[]=temp[0].split(",");
	    String word = docnames[1];//getting the input word
	    outKey.set(word);
	    String values = docnames[0]+","+docPart[0]+","+docPart[2]+","+"1";
	    //docnames[0] =documenet name
	    //docPart[0] word count
	    //docPart[2] normalised tf
	    //docPart[1] tf
	    //docnames[0] document number
	    
	    outValue.set(values);
            context.write(outKey,outValue);
	    
	  	}
    } 
    
    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {
    	private Text outValue1 = new Text();

        
    public void reduce(Text key, Iterable<Text> values, Context context) 
    	    throws IOException, InterruptedException
    	    {
    		
    		int m =0;
            ArrayList<String> AllValues=new ArrayList<String>();
    		int i=0;
    		for (Text text : values) {
    			AllValues.add(text.toString());
    		    String content[] = text.toString().split(",");
    		    m=m+Integer.parseInt(content[3]);
    		    		}
    		
    		
    			for (String text : AllValues) {
    				String content[] = text.toString().split(",");
    			    
    			    //float tf = Float.parseFloat(content[1]) / Integer.parseInt(content[2]);
    			    float tfidf_temp = (float)12/ m;
    			    
    			    String outkey = key +"," +content[0];
    			    double idf = Math.log(tfidf_temp)/Math.log(2);
    			    double tfMultiIdf = idf * Float.parseFloat(content[2]);
    			    String outvalue1 = m+","+content[2]+"," +idf +","+tfMultiIdf;
    			    context.write(new Text(outkey), new Text(outvalue1));
    			    //document,word is key
    			    // m, normalised tf, idf, tf*idf
    			}
    		}
    		

    				
    }
        
            

    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "PhaseOne");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setJarByClass(PhaseThree.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	//FileInputFormat.addInputPath(job, new Path(args[0]));
	//FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	/*added */
	String[] otherArgs = new GenericOptionsParser(conf, args)
	.getRemainingArgs();
	
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
	/*added ended*/
	
        
	job.waitForCompletion(true);
    }
        
}
