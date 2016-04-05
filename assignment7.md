<h1>
Output for shakespeare -
</h1>

<ol>
<li>1073319	   The slings and arrows of outrageous fortune </li>
</ol>

<h1>
Output for wiki -
</h1>

<p>

<ol>
<li>8468033 Eurostar    Eurostar is a high-speed railway service connecting London with Paris a... </li>
<li>88418750    Railway platform    A railway platform is a section of pathway, alongside rail trac... </li>
<li>137384453   Andy Bechtolsheim   Andreas "Andy" von Bechtolsheim (born September 30, 1955) is a... </li>
<li>299601817   List of civil parishes in Hampshire This is a list of civil parishes in the cere... </li>
<li>603734578   Institute for Quantum Computing The Institute for Quantum Computing, or IQC, loc... </li>
<li>986247632   List of University of Wisconsin–Madison people in academics List of University o… </li>
</ol>
</p>


<h2>NOTE - </h2>

<p>I believe "cs489-2016w-esnrRahman-a7-index-shakespeare" hbase table is corrupted because when I created it,
I accidentally ran </p>

`byte[] docId = ByteBuffer.allocate(keyPair.getRightElement()).array();`

<p>
So this might've messed it up as this code allocates waaayyy more than 4 bytes. Therefore the hbase table cannot get deleted now. Issue stated in piazza. https://piazza.com/class/ii64c6llmtx1xf?cid=319.
</p>

<p>
The script tries to run shakespeare with my username first. It gets stuck because of the issue stated above. But right now, whatever table gets created can get safely deleted. So my suggestion is to run shakespeare with a name other than "esnrRahman". And wiki can be normally run. Apologies for the inconvenience and messing up the hbase table. I manually ran everything multiple times and deleted from the hbase terminal to assure myself that the code is working.

I have uploaded a script so that it runs in altiscale. You can use that to do your tests.
P.S. - Please change the permissions first
</p>

Marks
BuildInvertedIndexHBase: 10/10
BooleanRetrievalHBase: 5/5
Shakespeare Sample: 5/5
Wiki sample: 5/5
Private: 5/5
Total: 30/30
