We have 5 versions of analyzer: seq, nolock, coarse, fine and noshare.

To run the program: 
1).First type 'make jar' to compile the jar file
2).Open the Makefile, and specify the path of input file(${path}) and parameter of Fibonacci function(${fibn}). e.g. fibn=1 makes Fibonacci spending little time, while fibn=20 makes Fibonacci spending long.
3).Copy your input file to the location you just specified
4).Choose one command from below, these commands will run each version 10 times and print out the 
average, max and min execution time:
1.to run programs of all five versions: make run-all
2.to run only sequential: make run-seq
3.to run only nolock: make run-nolock
4.to run only coarse lock: make run-coarse
5.to run only fine lock: make run-fine
6.to run only nosharing: make run-noshare

Notice: If you run make run-all or make run-nolock, there might be NullPointerException, this happens a lot in NO LOCK version because of race condition.

