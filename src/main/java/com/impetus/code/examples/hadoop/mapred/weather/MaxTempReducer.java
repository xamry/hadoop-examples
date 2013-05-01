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
package com.impetus.code.examples.hadoop.mapred.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <Prove description of functionality provided by this Type>
 * 
 * @author amresh.singh
 */
public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException
    {
        int maxValue = Integer.MIN_VALUE;
        for (IntWritable value : values)
        {
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new IntWritable(maxValue));
        
        
    }

}
