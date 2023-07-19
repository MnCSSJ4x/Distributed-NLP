# Distributed-NLP
An NLP pipeline for Parts of Speech tagging and indexing a document set using TF-IDF and Apache Hadoop. Implemented the Pairs and Stripes approach via Hadoop. For all the source code please visit ``` final/Project/src ``` in the repository. This has been tested on the Wikipedia-EN-20120601 ARTICLES.tar.gz corpus.

The following Analytics are available: 
1. Frequency of the POS tags
2. Document Frequency of words
3. Term Frequency
4. Inverse Document Frequency

The following are the required dependencies: 
1. Apache Hadoop
2. OpenNLP files: opennlp-tools-1.9.3.jar and opennlp-en-ud-ewt-pos-1.0-1.9.3.bin for the features like POS tagger and stemmer. Also the bin file consist of the pretrained POS tagger.

