package CS6240.WeatherAnalyzer;

import java.util.HashMap;
import java.util.Map;
/**
 * This is the Main function for executing program. The Main function reads the program name
 * to be executed(seq, nolock, coarse, fine or noshare), the file name as the input and
 * the input number of fibonacci function. It looks for the corresponding analyzer version,
 * load input file and give the fibonacci parameter to the analyzer.
 * @author caiyang
 *
 */
public class App 
{
	private static int RUN_TIMES = 10;
	private static Map<String, AbstractAnalyzer> MAP = new HashMap<>();
	static {
		MAP.put(Seq.NAME, new Seq());
		MAP.put(NoLock.NAME, new NoLock());
		MAP.put(NoSharing.NAME, new NoSharing());
		MAP.put(CoarseLock.NAME, new CoarseLock());
		MAP.put(FineLock.NAME, new FineLock());
	}
	
    public static void main(String[] args) throws Exception {
    	if (!MAP.containsKey(args[0]) && !args[0].equals("run-all")) {
        	throw new IllegalArgumentException("Can't find analyzer: " + args[0]);
        }
    	int fibn = Integer.parseInt(args[2]), succeed = 0;
    	long min = Long.MAX_VALUE, max = 0L, total_time = 0L;
    	// load the input file to List of String
    	MAP.get(args[0]).load(args[1]);
    	// execute the program RUN_TIMES time, if the program finishes without exception
    	// then succeed++, and record its run time.
    	for (int i = 0; i < RUN_TIMES; i++) {
    		try {
    			// res is the execution time
    			long res = MAP.get(args[0]).analyze(fibn);
    			total_time += res;
    			min = Math.min(min, res);
    			max = Math.max(max,  res);
    			succeed++;
    			System.out.println(res);
    		} catch (Exception e) {
    			System.err.println("error: " + e);
    		} finally {
    			MAP.get(args[0]).reset();
    		}
    	}
    	System.out.println("average time of " + args[0] + " : " + total_time / succeed);
    	System.out.println("max time of " + args[0] + " : " + max);
    	System.out.println("min time of " + args[0] + " : " + min);
    }
}
