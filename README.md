# DARCS
a distributed attribute reduction algorithm based on cosine similarity under the Spark framework 
Please, follow the following instructions to use DARCS.
# Installing
To be able to use the DARCS code, you will need to install the following:

Install Scala 2.11.12
Install Spark 2.4.7

# Main DARCS Parameters
rawDataRDD = The input data set
selectedAttributesIndex = The index of attributes in the candidate attribute subset

nbIterIfPerFeat = Number of iterations
sizeColumns = The number of attributes in each partitioned data block
