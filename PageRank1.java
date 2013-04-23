	import java.io.*;
     import java.util.*;    
    import java.lang.*;
    import java.net.*;
     import org.apache.hadoop.fs.Path;
     import org.apache.hadoop.conf.*;
    import org.apache.hadoop.io.*;
     import org.apache.hadoop.mapred.*;
     import org.apache.hadoop.util.*;
    import org.apache.hadoop.filecache.*;       
 import java.util.regex.*;
	
 	public class PageRank1{
			public static class linkgraph_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
 	     		private Text W = new Text();
			private Text F = new Text();
		
 	     	
 	     	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
 	       		String line = value.toString();
			String[] temp;
			String[] part_title;
			String[] temp2;
			String[] temp3;
			StringBuilder sb=new StringBuilder();
			Double d=0.85;
			Double pagerank=1-d;
		
			if(!(line.isEmpty())){
 			 	
				String title = line.substring(line.indexOf("<title>")+7,line.indexOf("</title>"));
                            
                           
                           	String delimiter="<text xml:space=\\\"preserve\\\">";
                           	temp2=line.split(delimiter);
                           	delimiter="</text>";
                            	temp3=temp2[1].split(delimiter);
                            

                          	Pattern pattern = Pattern.compile("(\\[\\[)(.*?)(\\]\\])");
                            	Matcher matcher = pattern.matcher(line);

                            	List<String> listMatches = new ArrayList<String>();

                             
				while(matcher.find())
                                {
				 listMatches.add(matcher.group(2));
				                                    
				}
				
				String pr=pagerank.toString();
				String p_r = pr.substring(0,5);	
                            	sb.append(p_r+"@#");
                            
			for(int i=0;i<listMatches.size();i++)
                            {
				String tem=listMatches.get(i); 
				String x;
			        int ii = tem.indexOf("|");
				if(ii!= -1){
					x = tem.substring(0,ii);
									
					}   
				else{
				 	x=tem;
				    }                   				
								
				if(i==0){                                
					sb.append(x);
                                }
                         	else
   				sb.append(","+x);
			 }
                           
			String link=sb.toString();
					
			W.set(title);	
			F.set(link);		
							
			output.collect(W,F);
 	       				
 	              }
 	   	}    
	}
 	 	
	public static class linkgraph_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>   {

		 private Text F = new Text(); 
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
 	      	
		while (values.hasNext())
		               {
		       		F = values.next();
				output.collect(key,F);		
				}	
		 	   }
		
		 	}	 	  







	 public static class pagerank_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	 	     	private Text W = new Text();
			private Text F = new Text();
		
 	     	
 	     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
 	       		
			String line = value.toString();
			String[] temp;
			String originalURL;
			String[] temp2;
			String[] temp3;
			Double current_pr,new_pr;
			int count;
			
			StringBuilder sb=new StringBuilder();
			
			
                            if(!(line.equals(" ")||line.equals("\t")||line.equals("\n")||line.isEmpty()))
                            {
			    	String delimiter ="\t";
                            	temp = line.split(delimiter);
                            	
				originalURL=temp[0];
				if(!originalURL.isEmpty()){
                            		delimiter="@#";
                            		temp2=temp[1].split(delimiter);
                            		current_pr=Double.parseDouble(temp2[0].trim());
                	      

					if(temp2.length==1)
						{
							W.set(originalURL);	
							F.set(temp2[0].trim());				   
							output.collect(W,F);
	
						}  

			     		else{
	                           		temp3=temp2[1].split(",");
	                           		count=temp3.length;

		                          	 if(count!=0){
		                           		new_pr=(current_pr/count);
			                           
							for(int i=0;i<count;i++){
                          
								W.set(temp3[i]);	
								F.set(Double.toString(new_pr));				   
								output.collect(W,F);
							}//end of for
                           			W.set(originalURL);	
						F.set("LINKS"+temp2[1]);				   
						output.collect(W,F);
                       	    		 	}//end of count
 	  				}  //end of else
				} //end of if original	
			}//end of line checl

 
	 }   
}
	public static class pagerank_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>   {

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
 	      	String links="",line="";
		Text F = new Text();
		String[] temp;
		String[] temp2;
		Double current_pr,new_pr=0.0,d=0.85;
		String val="";
		boolean test=false;
		
		while (values.hasNext())
               {
       			line= values.next().toString();				
			if(!(line.equals(" ")||line.equals("\t")||line.equals("\n"))){
		
			if(line.startsWith("LINKS"))
			{
			  temp=line.split("LINKS");
			 if(temp.length==2){
						
			links=temp[1];
			test=true;  			
			}
		}

		else	{
			
			current_pr=Double.parseDouble(line.trim());			
			new_pr+=current_pr;			
					
			}	
		    }		
		}
			if(test==true){
			new_pr=(1-d)+(d*(new_pr));
			val=Double.toString(new_pr);
			val=val+"@#"+links;
			F.set(val);		
			output.collect(key,F);
			}
			else{
				val=Double.toString(new_pr);
				F.set(val);				
				output.collect(key,F);	
			    }
		 
 	   	}
 	  }
 	   

	public static class sort_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
 	     	private Text W = new Text();
		private Text F = new Text();
		
 	     	
 	     	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
 	       		String line = value.toString();
			String[] temp;
			String originalURL;
			String[] temp2;
			String[] temp3;
			Double current_pr,new_pr;
			int count;
			
			StringBuilder sb=new StringBuilder();
			
			
 			
                            if(!(line.equals(" ")||line.equals("\t")||line.equals("\n")))
                            {
				if(!line.startsWith("LINKS"))			    
				{
				String delimiter ="\t";
                            	temp = line.split(delimiter);
                            	originalURL=temp[0];

                            	delimiter="@#";
                            	temp2=temp[1].split(delimiter);
                           
                        	W.set(originalURL);	
				 //float parseFloat = Float.parseFloat(temp2[0].trim());
 
        			
        			//FloatWritable rank = new FloatWritable(parseFloat);			
				F.set(temp2[0].trim());				   	
				output.collect(F,W);
                       	   	}
			  }
 	  
	       		
 	
 	}

  }   

	public static class sort_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>   {
		private Text F = new Text();
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
 	      	while (values.hasNext())
               {
       		F = values.next();
		output.collect(key,F);		
		}	
 	   }
 	  }
	 	  
	 public static void main(String[] args) throws Exception {
 	     JobConf conf = new JobConf(PageRank1.class); 
		 System.out.println("input");    
	     conf.setJobName("pagerank1");
 	
 	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(Text.class);
 	
 	     conf.setMapperClass(linkgraph_Map.class);
 	     //conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(linkgraph_Reduce.class);
 	
 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path("/prgout0"));
 	
 	     JobClient.runJob(conf);
 	  
	//job config for pagerank
	     for(int i=0;i<11;i++){
	     JobConf conf1 = new JobConf(PageRank1.class);     
	     System.out.println("pagerank"+i);
	     conf1.setJobName("pagerank"+i);
 	
 	     conf1.setOutputKeyClass(Text.class);
 	     conf1.setOutputValueClass(Text.class);
 	
 	     conf1.setMapperClass(pagerank_Map.class);
 	     //conf.setCombinerClass(Reduce.class);
 	     conf1.setReducerClass(pagerank_Reduce.class);
 	
 	     conf1.setInputFormat(TextInputFormat.class);
 	     conf1.setOutputFormat(TextOutputFormat.class);
 	
 	    FileInputFormat.setInputPaths(conf1, new Path("/prgout"+i+"/part-00000"));
 	    FileOutputFormat.setOutputPath(conf1, new Path("/prgout"+(i+1)));
 		//FileInputFormat.setInputPaths(conf1, new Path("/a0/part-00000"));
 	  	//FileOutputFormat.setOutputPath(conf1, new Path("/a2"));
 	
 	     JobClient.runJob(conf1);
 	   }		   
	//job config for sort
		
	    JobConf conf2 = new JobConf(PageRank1.class); 
		 System.out.println("sort");    
	     conf2.setJobName("sort");
 	
 	     conf2.setOutputKeyClass(Text.class);
 	     conf2.setOutputValueClass(Text.class);
 	
 	     conf2.setMapperClass(sort_Map.class);
 	     //conf.setCombinerClass(Reduce.class);
 	     conf2.setReducerClass(sort_Reduce.class);
 	
 	     conf2.setInputFormat(TextInputFormat.class);
 	     conf2.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf2, new Path("/prgout11/part-00000"));
 	     FileOutputFormat.setOutputPath(conf2, new Path("/finaloutput"));
 	 	conf2.setKeyFieldComparatorOptions("-k1.1r");
 	     JobClient.runJob(conf2);


	}
 }
