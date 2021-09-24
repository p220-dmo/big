package fr.htc.spark.amigo;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import fr.htc.spark.readers.Reader;
import scala.Tuple1;
import scala.Tuple2;

public class AmigoKpi {
	
	private static Reader<Integer, Amigo> amigoReader = new AmigoReader();
	
	public static void main(String[] args) {
		
		amigoReader.getObjectRdd()
		.mapToPair(amigo -> new Tuple2<Amigo, List<Integer>>(amigo, amigo.getBleus()))
		.flatMapToPair(new PairFlatMapFunction<Tuple2<Amigo,List<Integer>>, Amigo, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2< Amigo, Integer>> call(Tuple2<Amigo, List<Integer>> bleus) throws Exception {
				
				
				
				return res;
			}
		});//(t2 -> new Tuple2<Integer, Integer>(0, 1));
		
		
	}

}
