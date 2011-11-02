package org.goldenorb.algorithms.shorestPath;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

public class SampleDataGenerator extends TestCase {
	
	int nEdgePerVertex = 50;
	int nVertex = 100;
	int maxWeight = 10;
	int startVertexN = 5;
	String fileName = "resource/data/shorest_path_graph";
	
	public void test() {
		try {
			
			Random random = new Random();
			FileWriter writer = new FileWriter(fileName);
			for (int i = 0; i < nVertex; ++i) {
				int nEdge = random.nextInt(nEdgePerVertex) + 1;
				writer.write(random.nextInt(startVertexN) == 0 ? "bSrcVertex" : "normal");
				writer.write("\t"+"v" + i );
				for (int j = 0; j < nEdge; ++j) {
					int wgt, vt;
					do{
						wgt = random.nextInt(maxWeight) + 1;
						vt = random.nextInt(nVertex);
						writer.write("\t" + wgt + "@v" + vt);
					}while(vt == i);
				}
				writer.write("\n");
			}
			writer.flush();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public SampleDataGenerator() {
		
	}
	
	public SampleDataGenerator(int nv, int ne, int mw, String file) {
		this.nVertex = nv;
		this.nEdgePerVertex = ne;
		this.maxWeight = mw;
		this.fileName = file;
	}
	
	
	public static void main(String arg[]) {
		if (arg.length != 4) {
			System.out.println("usage:" + SampleDataGenerator.class.getCanonicalName() + " nVertex nEdgePerVertex maxWeight fileName");
			return ;
		}
		new SampleDataGenerator(Integer.parseInt(arg[0]), Integer.parseInt(arg[1]), Integer.parseInt(arg[2]), arg[3]).test();
	}

}
