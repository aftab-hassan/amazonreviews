import java.io.*;	
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class OneStepMatrixMultiplication {
 
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m"));
            int p = Integer.parseInt(conf.get("p"));
            String line = value.toString();
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (indicesAndValue[0].equals("A")) {
                for (int k = 0; k < p; k++) {
                    outputKey.set(indicesAndValue[1] + "," + k);
                    outputValue.set("A," + indicesAndValue[2] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            } else {
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + "," + indicesAndValue[2]);
                    outputValue.set("B," + indicesAndValue[1] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
 
    /* Remember : for each key, the reduce function is called */
    public static class Reduce extends Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
            
            //System.out.println("reduce function called,key=="+key.toString());
            
            //System.out.println("values=="+values);
            for (Text val : values) 
            {
                value = val.toString().split(",");
                //System.out.println("value[0]=="+value[0]+",value[1]=="+value[1]+",value[2]=="+value[2]);
                if (value[0].equals("A")) 
                {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else 
                {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }
            //System.out.println("hashA=="+hashA);
            //System.out.println("hashB=="+hashB);
            
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            float result = 0.0f;
            float a_ij;
            float b_jk;
            for (int j = 0; j < n; j++) 
            {
                a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result += a_ij * b_jk;
            }
       //     if (result != 0.0f) 
        //    {
                context.write(null, new Text(key.toString() + "," + Float.toString(result)));
          //  } 
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // A is an m-by-n matrix; B is an n-by-p matrix.
        //Earlier it was 2, 5, 3
        //first matrix should always be A, and m,n,p should be given in that order
        //this should be like 3,1000,3 or 3,XX,3 etc
        
        /**************************** <STEP 1 : XT.X> ****************************/
        conf.set("m", "3");
        conf.set("n", "298402");
        conf.set("p", "3");
 
        Job job = new Job(conf, "MatrixMatrixMultiplicationOneStep");
        job.setJarByClass(OneStepMatrixMultiplication.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path("steponeinput.txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(args[1]), true); // delete file, true for recursive 
     
        job.waitForCompletion(true);
        
        /* copying reducer output */
        File source = new File("/home/aftab/workspace/OneStepMatrixMultiplication/output/part-r-00000");
        File dest = new File("/home/aftab/Desktop/matrixinputgen/steponeoutput.txt");
        copyFileUsingFileStreams(source, dest);
        System.out.println("step one done");
        /**************************** </ STEP 1 : XT.X> ****************************/
   
        
        
        /**************************** < STEP 2 : stepone( to the power -1)> ****************************/
        /**************************** < STEP 2 *******************************************
         * 1. create matrix from output of stepone
         * 2. find inverse of this matrix
         * 3. write this with an A in front, to a file calling it /home/aftab/Desktop/matrixinputgen/steptwooutput.txt
         *  ******************************************************************************/
        /* 1. creating matrix from output of stepone */
        // The name of the file to open.
        String fileName = "/home/aftab/Desktop/matrixinputgen/steponeoutput.txt";
    	float[][] steponeoutput = new float[3][3];
    	
        // This will reference one line at a time
        String line = null;

        try 
        {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) 
            {
                String[] data = line.split(",");
                //System.out.println(data[0]);
                //System.out.println(data[1]);
                //System.out.println(data[2]);
                //System.out.println("");
                steponeoutput[Integer.parseInt(data[0])][Integer.parseInt(data[1])] = Float.parseFloat(data[2]);
                steponeoutput[Integer.parseInt(data[0])][Integer.parseInt(data[1])] = Float.parseFloat(data[2]);
                steponeoutput[Integer.parseInt(data[0])][Integer.parseInt(data[1])] = Float.parseFloat(data[2]);
            }    

            // Always close files.
            bufferedReader.close();            
        }
        catch(FileNotFoundException ex) 
        {
            System.out.println("Unable to open file '" + fileName + "'");                
        }
        catch(IOException ex) 
        {
            System.out.println("Error reading file '" + fileName + "'");                   
            // Or we could just do this: 
            // ex.printStackTrace();
        }
        
        /* 2. find the inverse of this matrix */
        float[][] steptwooutput = new float[3][3];
        float determinant=0;
        int  i,j;
        for(i=0;i<3;i++)
            determinant = determinant + (steponeoutput[0][i]*(steponeoutput[1][(i+1)%3]*steponeoutput[2][(i+2)%3] - steponeoutput[1][(i+2)%3]*steponeoutput[2][(i+1)%3]));

         for(i=0;i<3;i++)
         {
            for(j=0;j<3;j++)
            	steptwooutput[i][j] = (((steponeoutput[(i+1)%3][(j+1)%3] * steponeoutput[(i+2)%3][(j+2)%3]) - (steponeoutput[(i+1)%3][(j+2)%3]*steponeoutput[(i+2)%3][(j+1)%3]))/ determinant);
         }
        
         
        //3. write steptwooutput to a file calling it /home/aftab/Desktop/matrixinputgen/steptwooutput.txt
 		try 
 		{
 			//write to this file
 			File file = new File("/home/aftab/Desktop/matrixinputgen/steptwooutput.txt");
  
 			// if file doesnt exists, then create it
 			if (!file.exists()) 
 			{
 				file.createNewFile();
 			}
  
 			FileWriter fw = new FileWriter(file.getAbsoluteFile());
 			BufferedWriter bw = new BufferedWriter(fw);
 			
 			for(i=0;i<3;i++)
 			{
 				for(j=0;j<3;j++)
 				{
 					bw.write("A,"+Integer.toString(i)+","+Integer.toString(j)+","+Float.toString(steptwooutput[i][j])+"\n");
 					
 				}
 			}
 			bw.close();
  
 			System.out.println("Done");
  
 		} 
 		catch (IOException e) 
 		{
 			e.printStackTrace();
 		}
 		System.out.println("step two done");
 		/**************************** </STEP 2 : stepone( to the power -1)> ****************************/ 
 		
 		
 		
 		/**************************** < STEP 3 : steptwooutputdottxt.XTB> ****************************/
        /**************************** < STEP 3 *******************************************
         * 1. Merge steptwooutput.txt and XTB, call it stepthreeinput.txt 
         * 2. Do matrix multiplication using stepthreeinput.txt as the input file, call it stepthreeoutput.txt
         * 3. Write this output with an A in front as stepthreeoutputWithA.txt
         *  ******************************************************************************/
 		//1. Merge steptwooutput.txt and XTB, call it stepthreeinput.txt 		
 		String contentone = readFile("/home/aftab/Desktop/matrixinputgen/steptwooutput.txt");
 		String contenttwo = readFile("/home/aftab/Desktop/matrixinputgen/XTB");
 		String content = contentone + contenttwo;

 		try
        {
                File file = new File("/home/aftab/Desktop/matrixinputgen/stepthreeinput.txt");

                // if file doesnt exists, then create it
                if (!file.exists()) {
                        file.createNewFile();
                }

                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(content);
                bw.close();

                System.out.println("Done");
                source = new File("/home/aftab/Desktop/matrixinputgen/stepthreeinput.txt");
                dest = new File("/home/aftab/workspace/OneStepMatrixMultiplication/stepthreeinput.txt");
                copyFileUsingFileStreams(source, dest);

        }
        catch (IOException e)
        {
                e.printStackTrace();
        }

 		
 		//2. Do matrix multiplication using stepthreeinput.txt as the input file
 		conf.set("m", "3");
        conf.set("n", "3");
        conf.set("p", "298402");
        Job job3 = new Job(conf, "Step3");
        job3.setJarByClass(OneStepMatrixMultiplication.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapperClass(Map.class);
        job3.setReducerClass(Reduce.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path("stepthreeinput.txt"));
        FileOutputFormat.setOutputPath(job3, new Path("output3"));
        fs.delete(new Path("output3"), true); // delete file, true for recursive 
        job3.waitForCompletion(true);
        source = new File("/home/aftab/workspace/OneStepMatrixMultiplication/output3/part-r-00000");
        dest = new File("/home/aftab/Desktop/matrixinputgen/stepthreeoutput.txt");
        copyFileUsingFileStreams(source, dest);
        
        //3. Write stepthreeoutput.txt with an A in front as stepthreeoutputWithA
        //shell command : sed 's/^/A,/' /home/aftab/Desktop/matrixinputgen/stepthreeoutput.txt
        String finaloutput = "";
        
        /* This try statement should be optimised */
        try 
        {
        	System.out.println("came to the first try of the third part after mat mul");
        //sed 's/^/A,/' /home/aftab/Desktop/matrixinputgen/stepthreeoutput.txt
          String [] cmd = new String[] {"sed", "s/^/A,/", "/home/aftab/Desktop/matrixinputgen/stepthreeoutput.txt"};
          Process process = new ProcessBuilder(cmd).start();

          InputStream is = process.getInputStream();
          InputStreamReader isr = new InputStreamReader(is);
          BufferedReader br = new BufferedReader(isr);
          String lineRead;

          while ((lineRead = br.readLine()) != null) 
          {
        	  System.out.println("inside the for loop");
            finaloutput = finaloutput + lineRead + "\n";
            //System.out.println(lineRead);
          }

          //System.out.println(finaloutput);
        } 
        catch (IOException e) 
        {
          e.printStackTrace();
        }


        /* write to file part */
        try
        {
          content = finaloutput;
          //System.out.println(content);

          System.out.println("came to the line before stepthreeoutputWithA");
          File file = new File("/home/aftab/Desktop/matrixinputgen/stepthreeoutputWithA.txt");

          // if file doesnt exists, then create it
          if (!file.exists()) {
                  file.createNewFile();
          }

          FileWriter fw = new FileWriter(file.getAbsoluteFile());
          BufferedWriter bw = new BufferedWriter(fw);
          bw.write(content);
          bw.close();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
        System.out.println("step three done");
 		/**************************** </ STEP 3 : steptwo.XT> ****************************/
 		
 		
 		
 		
 		/**************************** < STEP 4 : stepthreeoutputdottxt.Y> ****************************/
        /**************************** < STEP 4 *******************************************
         * 1. 1. Merge stepthreeoutputWithA.txt and Y, call it stepfourinput.txt
         * 2. Do matrix multiplication using stepfourinput.txt as the input file
         * 3. You get the three co-efficients as a 3*1 vertical line, take these, save it somewhere
         *  ******************************************************************************/
        //1. Merge stepthreeoutputWithA.txt and Y, call it stepfourinput.txt 	
 		contentone = readFile("/home/aftab/Desktop/matrixinputgen/stepthreeoutputWithA.txt");
 		contenttwo = readFile("/home/aftab/Desktop/matrixinputgen/Y");
 		content = contentone + contenttwo;

 		try
        {
                File file = new File("/home/aftab/Desktop/matrixinputgen/stepfourinput.txt");

                // if file doesnt exists, then create it
                if (!file.exists()) 
                {
                        file.createNewFile();
                }

                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(content);
                bw.close();

                source = new File("/home/aftab/Desktop/matrixinputgen/stepfourinput.txt");
                dest = new File("/home/aftab/workspace/OneStepMatrixMultiplication/stepfourinput.txt");
                copyFileUsingFileStreams(source, dest);

        }
        catch (IOException e)
        {
                e.printStackTrace();
        }

 		
 		//2. Do matrix multiplication using stepfourinput.txt as the input file
 		conf.set("m", "3");
        conf.set("n", "298402");
        conf.set("p", "1");
        Job job4 = new Job(conf, "Step4");
        job4.setJarByClass(OneStepMatrixMultiplication.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapperClass(Map.class);
        job4.setReducerClass(Reduce.class);
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job4, new Path("stepfourinput.txt"));
        FileOutputFormat.setOutputPath(job4, new Path("output4"));
        fs.delete(new Path("output4"), true); // delete file, true for recursive 
        job4.waitForCompletion(true);
        source = new File("/home/aftab/workspace/OneStepMatrixMultiplication/output4/part-r-00000");
        dest = new File("/home/aftab/Desktop/matrixinputgen/stepfouroutput.txt");
        copyFileUsingFileStreams(source, dest);
        System.out.println("step four done");
        System.out.println("co-efficients obtained");
        
 		/**************************** </ STEP 4 : stepthreeoutputdottxt.Y> ****************************/
    }
    
    private static void copyFileUsingFileStreams(File source, File dest)
    		throws IOException {
    	InputStream input = null;
    	OutputStream output = null;
    	
    	try {
    		input = new FileInputStream(source);
    		output = new FileOutputStream(dest);
    		byte[] buf = new byte[1024];
    		int bytesRead;
    		while ((bytesRead = input.read(buf)) > 0) {
    			output.write(buf, 0, bytesRead);
    		}
    	} finally {
    		input.close();
    		output.close();
    	}
    }
    
    public static String readFile(String filename)
    {
       String content = null;
       File file = new File(filename); //for ex foo.txt
       try {
           FileReader reader = new FileReader(file);
           char[] chars = new char[(int) file.length()];
           reader.read(chars);
           content = new String(chars);
           reader.close();
       } catch (IOException e) {
           e.printStackTrace();
       }
       return content;
    }
    
    //not used
    public static void readfromfile()
    {
    	// The name of the file to open.
        String fileName = "/home/aftab/workspace/OneStepMatrixMultiplication/output/part-r-00000";
       
        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }    

            // Always close files.
            bufferedReader.close();            
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                   
            // Or we could just do this: 
            // ex.printStackTrace();
        }
    }
}