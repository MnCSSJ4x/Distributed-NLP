import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
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

public class term {
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Set<String> stopWords = new HashSet<>();
		PorterStemmer stemmer = new PorterStemmer();
		
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
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		    Map<String, Integer> tfMap = new HashMap<>();
		  
			
			if (line != null) {
				SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		    	String tokenizedLine[] = tokenizer.tokenize(line); //Tokenize line
		    	
		     
		    	
		    	
		    	for(String s: tokenizedLine) {
		    		 if (!stopWords.contains(s)){
		    			 String word = stemmer.stem(s.toLowerCase());
		    			 tfMap.put(word, tfMap.getOrDefault(word, 0) + 1);
		    		 }
		    	}
		    	
		    	for (Map.Entry<String, Integer> entry : tfMap.entrySet()) {
				      String term = entry.getKey();
				      int tf = entry.getValue();
				    
				      context.write(new Text(fileName + "\t" + term), new IntWritable(tf));
				    }
		    		
		    	
			}
			
			
		    
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		private Map<String, Integer> dfMap = new HashMap<>();

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	        URI[] files = context.getCacheFiles();
	        Path dfPath = new Path(files[1].getPath());
	        try (BufferedReader reader = new BufferedReader(new FileReader(dfPath.toString()))) {
	          String line;
	          while ((line = reader.readLine()) != null) {
	            String[] parts = line.split("\t");
	            String term = parts[0];
	            int df = Integer.parseInt(parts[1]);
	            dfMap.put(term, df);
	          }
	        }
	        
	      }
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
	
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			String term  = key.toString().split("\t")[1];
			int df = dfMap.getOrDefault(term, 0);
		    double score = sum * Math.log10(10000.0 / (df + 1));
			result.set(score);
		    context.write(key, result);
		
		}

	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//Creating Configuration
	    Job job = Job.getInstance(conf, "document frequency");
	    //Add link to Stopwords.txt 
	    job.addCacheFile(new URI("/Users/monjoy/Desktop/Assignment2/stopwords.txt"));
	    job.addCacheFile(new URI("/Users/monjoy/Desktop/Assignment2/out-wiki-50/part-r-00000"));
	    
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    //	    job.setOutputFormatClass(OutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
