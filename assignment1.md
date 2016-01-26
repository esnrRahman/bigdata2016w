<h1>Question 1.</h1>

<p>
For Pairs:
</p>
<p>
My implementation involves two jobs where the first job calculates each word count and counts all the non-empty lines. In the second job, all these counts are taken into account and based on the pair, the PMI is calculated. This calculation formula is used to calculate the PMI - log( ( N(x, y) * N ) / ( N(x) * N(y) )) where N(a) is the number of times a has occurred and N represents the number of non-empty lines read from the input file
</p>

<p>
The format that is used to output is a PairOfStrings that is a pair of two words that co-occur with each other and the DoubleWritable is the PMI
</p>

<p>
NOTE - Sidedata is generated in the form of PairOfStrings and IntWritable and saved as a SequenceFile. PairOfStrings contain (x, *) where x represents a word and the Int represents the number of times x has occurred. This sidedata is read by the setup code in the secondReducer in the form of SequenceFile and this intermediate information is used to calculate the PMI
</p>

<p>
NOTE 2 - After much struggle and by discussing it with a friend (acknowledgement goes to Ryan Moore), I realized that SequenceFiles need to be used instead of BufferedReader because thats how one reads from HDFS.
</p>

<ol>
<li>FirstJob Mapper input (K, V) - LongWritable, Text</li>
<li>FirstJob Mapper output (K, V) - PairOfStrings, IntWritable</li>

<li>FirstJob Combiner input (K, V) - PairOfStrings, IntWritable</li>
<li>FirstJob Combiner output (K, V) - PairOfStrings, IntWritable</li>

<li>FirstJob Reducer input (K, V) - PairOfStrings, IntWritable</li>
<li>FirstJob Reducer output (K, V) - PairOfStrings, IntWritable</li>
</ol>

<ol>
<li>SecondJob Mapper input (K, V) - LongWritable, Text</li>
<li>SecondJob Mapper output (K, V) - PairOfStrings, IntWritable</li>

<li>SecondJob Combiner input (K, V) - PairOfStrings, IntWritable</li>
<li>SecondJob Combiner output (K, V) - PairOfStrings, IntWritable</li>

<li>SecondJob Reducer input (K, V) - PairOfStrings, IntWritable</li>
<li>SecondJob Reducer output (K, V) - PairOfStrings, DoubleWritable</li>
</ol>

<p>
For Stripes:
</p>

<p>
Stripes follow a similar pattern that is used in pairs. There are two jobs and the first job is a complete copy from Pair. The sidedata is also created similarly. The main change is in the secondMapper and secondReducer because HMapStFW is used and necessary handling is done in the mapper and reducer to output in the proper format.
</p>

<p>
The format that is used to output is a Main key and a stripe where the stripe is the HMapStFW data structure. The stripe contains all the keys that is paired as a cooccurrence and the value is the PMI.
</p>

<ol>
<li>FirstJob Mapper input (K, V) - LongWritable, Text</li>
<li>FirstJob Mapper output (K, V) - PairOfStrings, IntWritable</li>

<li>FirstJob Combiner input (K, V) - PairOfStrings, IntWritable</li>
<li>FirstJob Combiner output (K, V) - PairOfStrings, IntWritable</li>

<li>FirstJob Reducer input (K, V) - PairOfStrings, IntWritable</li>
<li>FirstJob Reducer output (K, V) - PairOfStrings, IntWritable</li>
</ol>

<ol>
<li>SecondJob Mapper input (K, V) - LongWritable, Text</li>
<li>SecondJob Mapper output (K, V) - Text, HMapStFW</li>

<li>SecondJob Combiner input (K, V) - Text, HMapStFW</li>
<li>SecondJob Combiner output (K, V) - Text, HMapStFW</li>

<li>SecondJob Reducer input (K, V) - Text, HMapStFW</li>
<li>SecondJob Reducer output (K, V) - Text, HMapStFW</li>
</p>

<h1>Question 2.</h1>

<p>
Both implementations are ran in the linux.student.cs.uwaterloo.ca environment with the Shakespeare input file
</p>

<p>
Pairs ran in - 49.857s
</p>

<p>
Stripes ran in - 15.797s
</p>

<h1>Question 3.</h1>

<p>
Both implementations are ran in the linux.student.cs.uwaterloo.ca environment with the Shakespeare input file
</p>

<p>
Pairs ran in - 62.817s
</p>

<p>
Stripes ran in - 17.785s
</p>

<h1>Question 4.</h1>

<p> 38599 </p>

<h1>Question 5.</h1>

<p>
Highest PMI -
</p>

<p>
(maine, anjou)  3.5971175897745367
</p>

<p>
Script output -
</p>

<ol>
<li>(maine, anjou)  3.5971177</li>
<li>(anjou, maine)  3.5971177</li>
<li>(milford, haven)    3.5841527</li>
<li>(haven, milford)    3.5841527</li>
<li>(cleopatra's, alexandria)   3.5027547</li>
<li>(alexandria, cleopatra's)   3.5027547</li>
<li>(rosencrantz, guildenstern) 3.5022905</li>
<li>(guildenstern, rosencrantz) 3.5022905</li>
<li>(personae, dramatis)    3.49566</li>
<li>(dramatis, personae)    3.49566</li>
</ol>

<p>
High PMI indicates that the respective words in the pair occur very frequently. So this indicates that main, anjou occurs frequently in a line in the Shakespeare text but by itself, they may not occur as frequently.
</p>

<h1>Question 6.</h1>

<p>
Script output -
</p>

<ol>
<li>(tears, shed)   2.075765364455672</li>
<li>(tears, salt)   2.016787504496334</li>
<li>(tears, eyes)   1.1291422518865388</li>
<li>(death, father's)   1.0842273179991668</li>
<li>(death, die)    0.718134676579124</li>
<li>(death, life)   0.7021098794516143</li>
</ol>

<h1>Question 7.</h1>

<p>
Script output -
</p>

<ol>
<li>(waterloo, kitchener)   2.614997334922472</li>
<li>(waterloo, napoleon)    1.9084397672287545</li>
<li>(waterloo, napoleonic)  1.786618971523</li>
<li>(toronto, marlboros)    2.353996461343968</li>
<li>(toronto, spadina)  2.3126037761857425</li>
<li>(toronto, leafs)    2.3108905807416225</li>
</ol>


Q4p			1.5

Q4s			1.5

Q5p			1.5

Q5s			1.5

Q6.1p		1.5

Q6.1s		1.5

Q6.2p		1.5

Q6.2s		1.5

Q7.1p		1.5

Q7.1s		1.5

Q7.2p		1.5

Q7.2s		1.5

linux p		4

linux s		4

alti p		4

alti s		4

notes		

total		50

p stands for pair, s for stripe. linux p stands for run and compile pair in linux. 

If you have any question regarding to A1, plz come to DC3305 3~5pm on Friday (29th).
