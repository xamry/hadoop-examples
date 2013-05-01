/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.code.examples.hadoop.mapred.earthquake;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * http://www.ibm.com/developerworks/library/j-javadev2-15/ 
 * Data copied from http://earthquake.usgs.gov/earthquakes/feed/
 * Just run from eclipse, no need to run from command line
 * @author amresh.singh
 */
public class EarthQuakeAnalyzer
{
    public static void main(String[] args) throws Throwable {

        Job job = new Job();
        job.setJarByClass(EarthQuakeAnalyzer.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/eq/input"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/eq/output"));

        job.setMapperClass(EarthQuakeMapper.class);
        job.setReducerClass(EarthQuakeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
       }

}
