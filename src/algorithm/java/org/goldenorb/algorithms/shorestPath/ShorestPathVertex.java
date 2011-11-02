package org.goldenorb.algorithms.shorestPath;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.types.message.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShorestPathVertex extends
		Vertex<IntWritable, IntWritable, TextMessage> {
	private static final int DEFAULT_MAX_DISTANCE = 100;
	private static Logger LOG = LoggerFactory
			.getLogger(ShorestPathVertex.class);
	boolean bFirstSuperStep = true;
	int distance = Integer.MAX_VALUE;
	Integer maxDistance = null;
	Map<String, String> distanceMap = new HashMap<String, String>();

	public ShorestPathVertex() {
		super(IntWritable.class, IntWritable.class, TextMessage.class);
	}

	public ShorestPathVertex(String _vertexID, IntWritable _value,
			List<Edge<IntWritable>> _edges) {
		super(_vertexID, _value, _edges);
	}

	@Override
	public void compute(Collection<TextMessage> messages) {
		if (maxDistance == null) {
			String md = getOci().getOrbProperty("shorestPath.maxDistance");
			maxDistance = md == null ? DEFAULT_MAX_DISTANCE : Integer
					.getInteger(md);
		}
		boolean bSrc = this.getValue().get() == 1;		
		Map<String, String> modified = new HashMap<String, String>();
		if (bSrc && bFirstSuperStep) {
			bFirstSuperStep = false;
			distanceMap.put(getVertexID(), "0@1");
			modified.put(getVertexID(), "0@1");
//			LOG.debug("if modified.entrySet()\t"+modified.entrySet().size());
		} else {
			/**
			 * message format : distance-to-source-vertex @ path_count @
			 * source_vertex_id
			 */
  
			for (TextMessage msg : messages) {
				LOG.debug("MESSAGE:"+msg.get());
				String val = msg.get();
				String[] vals = val.split("@");
				int _d = Integer.parseInt(vals[0]);
				if (distanceMap.get(vals[2]) == null) {
					distanceMap.put(vals[2], vals[0] + "@1");
					modified.put(vals[2], vals[0] + "@1");
				} else {
					String[] distmap = distanceMap.get(vals[2]).split("@");
					Integer dist = Integer.parseInt(distmap[0]);
					if (_d < dist) {
						distanceMap.put(vals[2], vals[0] + "@" + vals[1]);
						modified.put(vals[2], vals[0] + "@" + vals[1]);
					} else if (_d == dist) {
						distanceMap.put(vals[2],
								vals[0]
										+ "@"
										+ Integer.toString((Integer
												.parseInt(vals[1]) + Integer
												.parseInt(distmap[1]))));
						modified.put(vals[2],
								vals[0]
										+ "@"
										+ Integer.toString((Integer
												.parseInt(vals[1]) + Integer
												.parseInt(distmap[1]))));
					} else {
						continue;
					}
				}
			}
		}
	//		LOG.debug("else modified.entrySet()\t"+modified.entrySet().size());
			for (Map.Entry<String, String> map : modified.entrySet()) {
				String[] distCount = map.getValue().split("@");
	
				for (Edge<IntWritable> e : getEdges()) {
					int targetDistance = Integer.parseInt(distCount[0])
							+ e.getEdgeValue().get();
					if (targetDistance > maxDistance)
						continue;
					String m = Integer.toString(targetDistance) + "@"
							+ distCount[1] + "@" + getVertexID();
					sendMessage(new TextMessage(e.getDestinationVertex(), m));
				}
			}
			voteToHalt();
	}


	public int getDistance() {
		return distance;
	}

	public Map<String, String> getDistanceMap() {
		return distanceMap;
	}
}
