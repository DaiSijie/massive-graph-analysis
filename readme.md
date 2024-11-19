# MapReduce Implementation of the Server Flow Coreset

This is a java implementation of the [server flow coreset](https://arxiv.org/abs/2011.06481), which is suitable for deployment on e.g. AWS.

## How to Run the Code

The source code is located in the ```code``` folder and consists of standard Java files. Various dataset cleaners are located in the ```datasets``` folder. 

1. Make sure that you can run the Apache Hadoop [word count example](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0) on your machine. You need to use java JDK11. 
2. Make sure that you can compile the code located in  the code folder. You will need the Apache Hadoop library as well as JGraphT. Alternatively, you can use the pre-compiled ```HadoopServerFlowCoreset.jar```. It uses one mapper and two reducers in the first round.
3. To verify that everything is set correctly, copy ```/datasets/testgraph.txt``` 
to your input folder and run the program using the following command:
```/path/to/hadoop jar /path/to/compiled.jar /path/to/inputfolder /path/to/outputfolder```
4. The output is located in ```path/to/outputfolder```. The intermediate folder contains all the coresets and the other contains the final matching.
5. You should get a matching of size about 360 on the test graph.

## Acquiring and cleaning datasets

We describe here how to aquire and clean the datasets. You can then run our program on those by using the same command as above.

### Google+ data
1. Download **ego-Gplus** from https://snap.stanford.edu (only the file named ```gplus_combined.txt.gz```).
2. Run ```gplusclean.py```:
```python3 gplusclean.py /path/to/downloaded.txt /path/to/destination.txt```
3. Make the graph bipartite using for instance ```bipartify.py```:
```python3 bipartify /path/to/cleaned.txt /path/to/bipartite.txt```

### DG-miner
1. Download **DG-miner** from https://snap.stanford.edu
2. Run ```dgminerclean.py```:
```python3 dgminercleaning.py /path/to/downloaded.tsv /path/to/desination.txt```
3. The cleaned file contains the **DG-miner** graph which is already bipartite.

### Neuron & Cardiac Muscle
1. Download ```GG-NE.tar.gz``` from https://snap.stanford.edu
2. Extract **Neuron** and **Cardiac Muscle** from the archive
3. Run ```tissueclean.py```:
```python3 tissueclean.py /path/to/extracted.tsv /path/to/dest.txt```
4. Make the graph bipartite using for instance ```bipartify.py```:
```python3 bipartify /path/to/cleaned.txt /path/to/bipartite.txt```

## Code Highlights

A custom implementation of the Ford-Fuklerson algorithm with stopping heuristic
