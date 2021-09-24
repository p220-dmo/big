import java.util.HashMap;
import java.util.Map;

public class Test {

	public static void main(String[] args) {
		
		Map<Object, Integer> map = new HashMap<Object, Integer>();
		Object o1 = new Object();
		Object o2 = o1;
		map.put(o1, 1);
		map.put(o2, 2);

		System.out.println(map.get(o1));
		System.out.println(map.get(o2));
		System.out.println(map.size());
	}

}
