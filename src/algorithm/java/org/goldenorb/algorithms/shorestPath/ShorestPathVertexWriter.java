package org.goldenorb.algorithms.shorestPath;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.goldenorb.io.output.OrbContext;
import org.goldenorb.io.output.VertexWriter;
import org.hsqldb.lib.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShorestPathVertexWriter extends
 
	VertexWriter<ShorestPathVertex, Text, Text> {
//	private static Logger LOG = LoggerFactory.getLogger(ShorestPathVertexWriter.class);
	

	@Override 

	public OrbContext<Text, Text> vertexWrite(ShorestPathVertex vertex) {
		// TODO Auto-generated method stub
//		LOG.debug("write '" + vertex.getVertexID() + "', getDistanceMap size" + vertex.getDistanceMap().size());
		String s = new String();	
		OrbContext<Text, Text> orbContext = new OrbContext<Text, Text>();
		for (Map.Entry<String, String> map : vertex.getDistanceMap().entrySet()) {
			s += map.getValue() + "@" + map.getKey()+"\t";
		}
		
		orbContext.write(new Text(vertex.getVertexID()), new Text(s));
		return orbContext;
	}

}
