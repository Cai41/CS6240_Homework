package CS6240.WeatherAnalyzer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * NoSharing does not use any share object to track result. Each worker has its own data structrue.
 * It will aggregate all the results after all workers finish.
 * @author caiyang
 *
 */
public class NoSharing extends AbstractAnalyzer {
	public static String NAME = "noshare";
	private int fibn;
	
	/**
	 * A worker is a thread will process a portion of data, which is sublist
	 * @author caiyang
	 *
	 */
	class Worker implements Runnable {
		private List<String> sublist;
		private Map<String, Node> statByStation;
		
		public Worker(List<String> sublist) {
			this.sublist = sublist;
			this.statByStation = new HashMap<>();
		}

		@Override
		public void run() {
			for (String s : sublist) {
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
		}
	}
	
	public NoSharing() {
		statByStation = new HashMap<>();
	}
	
	@Override
	public long analyze(int fibn) throws Exception {
		this.fibn = fibn;
		// evenly distribute the work. In average, each worker has
		// numEntriesPerWorker lines to process, but numMoreWorks of them have one more
		// lines to process(1 +mnumEntriesPerWorker)
		int numEntriesPerWorker = list.size() / THREAD_SIZE, numMoreWorks = list.size() % THREAD_SIZE;
		Worker[] workers = new Worker[THREAD_SIZE];
		int endIndex = 0;
		// assign each work a sublist to process, which is list[endIndex ... newEndIndex]
		for (int i = 0; i < THREAD_SIZE; i++) {
			int newEndIndex = endIndex + numEntriesPerWorker + (i < numMoreWorks ? 1 : 0);
			workers[i] = new Worker(list.subList(endIndex, newEndIndex));
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
		// aggregate all the results from workers
		for (Worker w : workers) {
			Map<String, Node> map = w.statByStation;
			for (Map.Entry<String, Node> e : map.entrySet()) {
				String k = e.getKey();
				Node n = e.getValue();
				if (!statByStation.containsKey(k)) statByStation.put(k, new Node());
				Node n1 = statByStation.get(k);
				n1.sum += n.sum;
				n1.count += n.count;
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
