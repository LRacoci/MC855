package org.hipi.examples;

import org.hipi.mapreduce.BinaryOutputFormat;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.opencv.OpenCVUtils;
import org.hipi.opencv.OpenCVMatWritable;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.opencv_core.Size;

import java.io.IOException;

public class Gaussiana extends Configured implements Tool {

  public static final int sizeX = 5;
  public static final int sizeY = 5;
  
  public static class GaussianaMapper extends Mapper<HipiImageHeader, FloatImage, IntWritable, OpenCVMatWritable> {
    
    public void map(HipiImageHeader key, FloatImage image, Context context) 
        throws IOException, InterruptedException {

      Mat cvImageRGB = OpenCVUtils.convertRasterImageToMat(image);
      Mat cvGaussianRGB = new Mat();
      opencv_imgproc.blur(cvImageRGB, cvGaussianRGB, new Size(sizeX, sizeY));
      context.write(new IntWritable(cvGaussianRGB.hashCode()), new OpenCVMatWritable(cvGaussianRGB));

    } // map()

  } // GaussianaMapper
  
  public static class GaussianaReducer extends Reducer<IntWritable, OpenCVMatWritable, NullWritable, OpenCVMatWritable> {

    public void reduce(IntWritable key, Iterable<OpenCVMatWritable> values, Context context)
        throws IOException, InterruptedException {

      for (OpenCVMatWritable value : values) {
        context.write(NullWritable.get(), value);
      }

    } // reduce()

  } // GaussianaReducer
  
  public int run(String[] args) throws Exception {
    // Check input arguments
    if (args.length != 2) {
      System.out.println("Usage: gaussiana <input HIB> <output directory>");
      System.exit(0);
    }
    
    // Initialize and configure MapReduce job
    Job job = Job.getInstance();
    // Set input format class which parses the input HIB and spawns map tasks
    job.setInputFormatClass(HibInputFormat.class);
    // Set the driver, mapper, and reducer classes which express the computation
    job.setJarByClass(Gaussiana.class);
    job.setMapperClass(GaussianaMapper.class);
    job.setReducerClass(GaussianaReducer.class);
    // Set the types for the key/value pairs passed to/from map and reduce layers
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(OpenCVMatWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OpenCVMatWritable.class);
    job.setOutputFormatClass(BinaryOutputFormat.class);
    
    // Set the input and output paths on the HDFS
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Execute the MapReduce job and block until it complets
    boolean success = job.waitForCompletion(true);
    
    // Return success or failure
    return success ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Gaussiana(), args);
    System.exit(0);
  }
  
}