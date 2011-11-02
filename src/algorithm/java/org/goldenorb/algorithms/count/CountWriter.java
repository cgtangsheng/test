package org.goldenorb.algorithms.count;

import org.apache.hadoop.io.Text;
import org.goldenorb.io.output.OrbContext;
import org.goldenorb.io.output.VertexWriter;

public class CountWriter extends VertexWriter<CountVertex, Text, Text> {

	@Override
	public OrbContext<Text, Text> vertexWrite(CountVertex vertex) {
		/**
		 * output format
		 * vertexID \t indgree \t outdegree
		 */
		OrbContext<Text, Text> orbContext = new OrbContext<Text, Text>();
		String value = vertex.getInDegree() + "\t" + vertex.getOutDegree();
		orbContext.write(new Text(vertex.getVertexID()), new Text(value));
		
		return orbContext;
	}

}