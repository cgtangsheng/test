package org.goldenorb.algorithms.shorestPath;

import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.io.input.VertexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShorestPathVertexReader extends
		VertexBuilder<ShorestPathVertex, LongWritable, Text> {
	private static Logger LOG = LoggerFactory
			.getLogger(ShorestPathVertexReader.class);

	@Override
	protected ShorestPathVertex buildVertex(LongWritable key, Text value) {
		// TODO Auto-generated method stub
		/**
		 * format : v0 \t 12@v1 \t 5@v2
		 */

		ArrayList<Edge<IntWritable>> edgeCollection = new ArrayList<Edge<IntWritable>>();
		String[] values = value.toString().split("\t");

		boolean bSrcVertex = "bSrcVertex".equals(values[0]);
		String srcVertex = values[1];
		if (bSrcVertex) {
	//		LOG.debug("ShorestPathVertexReader SrcVertex=" + bSrcVertex
		//			+ srcVertex);
		}
		for (int i = 2; i < values.length; ++i) {
			String[] vals = values[i].split("@");
			int wgt = Integer.parseInt(vals[0]);
			String distVertex = vals[1];
			Edge<IntWritable> e = new Edge<IntWritable>(distVertex,
					new IntWritable(wgt));
			edgeCollection.add(e);
		}

		return new ShorestPathVertex(srcVertex, new IntWritable(bSrcVertex ? 1 :0),
				edgeCollection);
	}

}
