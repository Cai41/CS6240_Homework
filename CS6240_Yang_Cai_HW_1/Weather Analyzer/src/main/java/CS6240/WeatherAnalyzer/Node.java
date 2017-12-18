package CS6240.WeatherAnalyzer;

/**
 * This class represents an entry in HashMap, record the sum temperatrue and count of one station.
 * @author caiyang
 *
 */
public class Node {
	int count;
	int sum;
	
	public Node() {
		count = 0;
		sum = 0;
	}
	
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public int getSum() {
		return sum;
	}
	public void setSum(int sum) {
		this.sum = sum;
	}
}
