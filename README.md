# Migrate HDFS Store to Azure Data Lake Storage Gen2

The key challenge for customers with existing on-premises Hadoop clusters that wish to migrate to Azure (or exist in a hybrid environment) is the movement of the existing dataset. The dataset may be very large, which likely rules out online transfer. Transfer volume can be solved by using Azure Databox as a physical appliance to 'ship' the data to Azure.

This set of scripts provides specific support for moving big data analytics datasets from an on-premises HDFS cluster to ADLS Gen2 using a variety of Hadoop and custom tooling.

This method assumes the presence of a Hadoop cluster in the on-premises environment (ie. the source of the HDFS data) and a Hadoop cluster in Azure (eg. HDInsight). Hadoop is required for distributed copy (distcp).



