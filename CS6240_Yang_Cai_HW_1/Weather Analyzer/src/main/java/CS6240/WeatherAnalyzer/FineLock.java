package CS6240.WeatherAnalyzer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Fine lock will only lock one entry in hashmap when neccessary. It uses ConcurrentHashMap
 * to avoid locking the whole table and race condition.
 * @author caiyang
 *
 */
public class FineLock extends AbstractAnalyzer {
	public static String NAME = "finelock";
	private int fibn;
	
	/**
	 * A worker is a thread will process a portion of data, which is sublist
	 * @author caiyang
	 *
	 */
	class Worker implements Runnable {

		private List<String> sublist;
		private Map<String, Node> statByStation;
		
		public Worker(List<String> sublist, Map<String, Node> statByStation) {
			this.sublist = sublist;
			this.statByStation = statByStation;
		}

		@Override
		public void run() {
			for (String s : sublist) {
				String[] ss = s.split(",");
				if (ss[2].equals("TMAX")) {
					// ConcurrentHashMap.putIfAbsent is atomic
					if (!statByStation.containsKey(ss[0])) {
						statByStation.putIfAbsent(ss[0], new Node());
					}
					Node n = statByStation.get(ss[0]);
					// we only lock the object that stores the information for one station
					// rather than the whole hashmap
					synchronized (n) {
						try {
							int temp = Integer.parseInt(ss[3]);
							n.sum += temp;
							n.count++;
							fib(fibn);
						} catch (NumberFormatException e) {
							continue;
						}
					}
				}
			}
		}
	}
	
	public FineLock() {
		statByStation = new ConcurrentHashMap<>();
	}
	
	@Override
	public long analyze(int fibn) throws Exception {
		this.fibn = fibn;
		// evenly distribute the work. In average, each worker has
		// numEntriesPerWorker lines to process, but numMoreWorks of them have one more
		// lines to process(1 +mnumEntriesPerWorker)
		int numEntriesPerWorker = list.size() / THREAD_SIZE, numMoreWorks = list.size() % THREAD_SIZE;
		int endIndex = 0;
		Worker[] workers = new Worker[THREAD_SIZE];
		// assign each work a sublist to process, which is list[endIndex ... newEndIndex]
		for (int i = 0; i < THREAD_SIZE; i++) {
			int newEndIndex = endIndex + numEntriesPerWorker + (i < numMoreWorks ? 1 : 0);
			workers[i] = new Worker(list.subList(endIndex, newEndIndex), statByStation);
			endIndex = newEndIndex;
		}
		ExecutorService pool = Executors.newFixedThreadPool(THREAD_SIZE);
		Future<?>[] futures = new Future[THREAD_SIZE];
		long start = System.currentTimeMillis();
		// kick off each thread
		for (int i = 0; i < THREAD_SIZE; i++) {
			futures[i] = pool.submit(workers[i]);
		}
		// wait for all thread to finish
		try {
			pool.shutdown();
			pool.awaitTermination(2, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			System.err.println(e);
		} finally {
			pool.shutdownNow();
		}
		// If any thread throws a exception, the futures[i].get() will throw exception
		// then pass the exception to the caller of Coarse.analyze, to inform that it doesn't 
		// finish correctly
		for (int i = 0; i < THREAD_SIZE; i++) {
			futures[i].get();
		}
		getAverages();
		long end = System.currentTimeMillis();
		return end - start;
	}

	@Override
	public void reset() {
		statByStation = new ConcurrentHashMap<>();
	}
}
