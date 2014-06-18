package org.ydc.desman;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class GeneticInputFormat
extends SequenceFileInputFormat<BytesWritable, BytesWritable> {
}
