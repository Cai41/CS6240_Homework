package CS6240.WeatherAnalyzer;

import java.util.HashMap;

/** 
 * Seq does not have any parallism, processes the lines one by one.
 * @author caiyang
 *
 */
public class Seq extends AbstractAnalyzer{
	public static String NAME = "seq";
	
	public Seq() {
		statByStation = new HashMap<>();
	}
	
	@Override
	public long analyze(int fibn) throws Exception {
		long start = System.currentTimeMillis();
		for (String s : list) {
			String[] ss = s.split(",");
			if (ss[2].equals("TMAX")) {
				try {
					int temp = Integer.parseInt(ss[3]);
					if (!statByStation.containsKey(ss[0])) statByStation.put(ss[0], new Node());
					Node n = statByStation.get(ss[0]);
					n.sum += temp;
					n.count++;
					fib(fibn);
				} catch (NumberFormatException e) {
					continue;
				}
			}
		}
		getAverages();
		long end = System.currentTimeMillis();
		return end - start;
	}

	@Override
	public void reset() {
		statByStation = new HashMap<>();
	}
}
