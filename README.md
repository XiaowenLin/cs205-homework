Harvard CS205 Homework Repository  

http://iacs-courses.seas.harvard.edu/courses/cs205/homework.html  

Self-study extreme programming course offered at Harvard, as a gradute level course for data science program. 

1. Do the final question in HW1. In map-reduce programming paradigm, use spark to find independent components in a graph. The essence is implementation of decentralized BFS. Init distance dict, and rdd of nodes. In each iteration, in map prase, nodes at the frontier updates its distance. In reduce phase, collect dict of distance, which will be used in the next map phase. After BFS is implemented, use it to get indenpendent component in the graph. Pop one node, do BFS search, collect the touched nodes as one component, and remove them from the pool. Repeat until pool is empty. 

2. Do P4 and p5. Use threading module to parallelize median filter image processing in Cython. The difficult part is coordination between threads: thread i has to wait for thread i - 1 and thread i + 1 finish iteration n - 1, in order to proceed to iteration i. Intuitive and simple idea is that, put all these events into a matrix, with different rows representing different iterations and different columns for different iterations, so that between proceeding, thread can check whether its neighbors has accomplished i - 1 iteration or not. 
