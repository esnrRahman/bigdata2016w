<h1>Question 1.</h1>

<p>

For Pairs:
My implementation involves two jobs where the first job calculates each word count and counts all the non-empty lines. In the second job, all these counts are taken into account and based on the pair, the PMI is calculated. This calculation formula is used to calculate the PMI - log( ( N(x, y) * N ) / ( N(x) * N(y) )) where N(a) is the number of times a has occurred and N represents the number of non-empty lines read from the input file

The format that is used to output is a PairOfStrings that is a pair of two words that co-occur with each other and the DoubleWritable is the PMI

NOTE - Sidedata is generated in the form of PairOfStrings and IntWritable and saved as a SequenceFile. PairOfStrings contain (x, *) where x represents a word and the Int represents the number of times x has occurred. This sidedata is read by the setup code in the secondReducer in the form of SequenceFile and this intermediate information is used to calculate the PMI

NOTE 2 - After much struggle and by discussing it with a friend (acknowledgement goes to Ryan Moore), I realized that SequenceFiles need to be used instead of BufferedReader because thats how one reads from HDFS.

FirstJob Mapper input (K, V) - LongWritable, Text
FirstJob Mapper output (K, V) - PairOfStrings, IntWritable

FirstJob Combiner input (K, V) - PairOfStrings, IntWritable
FirstJob Combiner output (K, V) - PairOfStrings, IntWritable

FirstJob Reducer input (K, V) - PairOfStrings, IntWritable
FirstJob Reducer output (K, V) - PairOfStrings, IntWritable

SecondJob Mapper input (K, V) - LongWritable, Text
SecondJob Mapper output (K, V) - PairOfStrings, IntWritable

SecondJob Combiner input (K, V) - PairOfStrings, IntWritable
SecondJob Combiner output (K, V) - PairOfStrings, IntWritable

SecondJob Reducer input (K, V) - PairOfStrings, IntWritable
SecondJob Reducer output (K, V) - PairOfStrings, DoubleWritable

For Stripes:

Stripes follow a similar pattern that is used in pairs. There are two jobs and the first job is a complete copy from Pair. The sidedata is also created similarly. The main change is in the secondMapper and secondReducer because HMapStFW is used and necessary handling is done in the mapper and reducer to output in the proper format.

The format that is used to output is a Main key and a stripe where the stripe is the HMapStFW data structure. The stripe contains all the keys that is paired as a cooccurrence and the value is the PMI.

FirstJob Mapper input (K, V) - LongWritable, Text
FirstJob Mapper output (K, V) - PairOfStrings, IntWritable

FirstJob Combiner input (K, V) - PairOfStrings, IntWritable
FirstJob Combiner output (K, V) - PairOfStrings, IntWritable

FirstJob Reducer input (K, V) - PairOfStrings, IntWritable
FirstJob Reducer output (K, V) - PairOfStrings, IntWritable

SecondJob Mapper input (K, V) - LongWritable, Text
SecondJob Mapper output (K, V) - Text, HMapStFW

SecondJob Combiner input (K, V) - Text, HMapStFW
SecondJob Combiner output (K, V) - Text, HMapStFW

SecondJob Reducer input (K, V) - Text, HMapStFW
SecondJob Reducer output (K, V) - Text, HMapStFW

</p>

<h1>Question 2.</h1>

<p> Both implementations are ran in the linux.student.cs.uwaterloo.ca environment with the Shakespeare input file 

Pairs ran in - 49.857s

Stripes ran in - 15.797s 

</p>

<h1>Question 3.</h1>

<p> Both implementations are ran in the linux.student.cs.uwaterloo.ca environment with the Shakespeare input file 

Pairs ran in - 62.817s

Stripes ran in - 17.785s

</p>

<h1>Question 4.</h1>

<p> 38599 </p>

<h1>Question 5.</h1>

<p> 

Highest PMI - 

(maine, anjou)  3.5971175897745367

Script output - 

(maine, anjou)  3.5971177
(anjou, maine)  3.5971177
(milford, haven)    3.5841527
(haven, milford)    3.5841527
(cleopatra's, alexandria)   3.5027547
(alexandria, cleopatra's)   3.5027547
(rosencrantz, guildenstern) 3.5022905
(guildenstern, rosencrantz) 3.5022905
(personae, dramatis)    3.49566
(dramatis, personae)    3.49566

High PMI indicates that the respective words in the pair occur very frequently. So this indicates that main, anjou occurs frequently in a line in the Shakespeare text but by itself, they may not occur as frequently.

</p>

<h1>Question 6.</h1>

Script output - 

(tears, shed)   2.075765364455672
(tears, salt)   2.016787504496334
(tears, eyes)   1.1291422518865388
(death, father's)   1.0842273179991668
(death, die)    0.718134676579124
(death, life)   0.7021098794516143

<h1>Question 7.</h1>

Script output - 
(waterloo, kitchener)   2.614997334922472
(waterloo, napoleon)    1.9084397672287545
(waterloo, napoleonic)  1.786618971523
(toronto, marlboros)    2.353996461343968
(toronto, spadina)  2.3126037761857425
(toronto, leafs)    2.3108905807416225

