import java.io.File;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;


import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.stemmer.PorterStemmer;

public class DocumentFrequency {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	    private Set<String> stopWords = new HashSet<>();
	    private Text docid = new Text();
		
	    public void setup(Context context) throws IOException, InterruptedException {
	        // Load stopwords from file
	        URI[] cacheFiles = context.getCacheFiles();
	        if (cacheFiles != null && cacheFiles.length > 0) {
	          try (BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
	            String line;
	            while ((line = reader.readLine()) != null) {
	              stopWords.add(line.trim());
	            }
	          }
	        }
	      }
	    

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			PorterStemmer stemmer = new PorterStemmer();
			String line = value.toString();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		    docid.set(fileName);
			
			
			if (line != null) {
				SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		    	String tokenizedLine[] = tokenizer.tokenize(line); //Tokenize line
		    	
		    	 
		    	
		    	
		    	for(String s: tokenizedLine) {
		    		 if (!stopWords.contains(s)){
		    			 String stemmedOutput = stemmer.stem(s.toLowerCase());
				    		word.set(stemmedOutput);
							context.write(word, docid);
		    			 
		    		 }
		    	}
		    }
				
		}
		
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Set<String> docids = new HashSet<String>();
			//Checking for unique document only 
			for (Text val : values) {
				docids.add(val.toString());
			}
			result.set(docids.size());
		    context.write(key, result);
		
		}

	}
//	public static class OutputFormat extends TextOutputFormat<Text, Text> {
//
//		  @Override
//		  public void writeRecord(CSVPrinter printer, Entry<Text, Text> entry) throws IOException {
//		    printer.print(entry.getKey().toString());
//		    printer.print('#');
//		    printer.print(entry.getValue().toString());
//		    printer.println();
//		  }
//		}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//Creating Configuration
	    Job job = Job.getInstance(conf, "document frequency");
	    //Add link to Stopwords.txt 
	    job.addCacheFile(new URI("/Users/monjoy/Desktop/Assignment2/stopwords.txt"));
	    
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    //	    job.setOutputFormatClass(OutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
