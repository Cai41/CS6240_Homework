package CS6240.WeatherAnalyzer;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This is the abstract class for all versions of analyzer(seq, nolock, coarse, fine and noshare)
 * @author caiyang
 *
 */
public abstract class AbstractAnalyzer {
	// map each stationId to its corresponding statistics
	protected Map<String, Node> statByStation;
	// we have 3 threads in parallel versions
	protected static int THREAD_SIZE = 3;
	// list holds all lines of input files
	protected List<String> list;
	
	/** 
	 * begin parsing and prcessing the weather data
	 * @param fibn the parameter of Fibonacci function
	 * @return exection time
	 * @throws Exception
	 */
	abstract public long analyze(int fibn) throws Exception;
	
	/** 
	 * compute average TMAX for all stations
	 * @return Map that maps stationId to its average temperature
	 */
	public Map<String, Double> getAverages() {
		Map<String, Double> res = new HashMap<>();
		for (Map.Entry<String, Node> entry : statByStation.entrySet()) {
			res.put(entry.getKey(), entry.getValue().sum * 1.0 / entry.getValue().count);
		}
		return res;
	}
	
	/**
	 *  reset inner state such as hasmap, etc.
	 */
	abstract public void reset();
	
	/**
	 *  load the input file, read all lines to list
	 * @param filename to load
	 * @throws Exception
	 */
	public void load(String filename) throws Exception {
		list = new ArrayList<>();
		try (Stream<String> stream = Files.lines(Paths.get(filename))) {
			stream.forEach(list :: add);
		}
	}
	
	/** 
	 * Fibonacci function
	 * @param n
	 * @return
	 */
	public int fib(int n) {
		if (n <= 2) return 1;
		return fib(n - 1) + fib(n - 2);
	}
}
