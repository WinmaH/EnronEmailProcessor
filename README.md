# EnronEmailProcessor

Build command
mvn clean install -Dcheckstyle.skip=true -Dfindbugs.skip=true


Download and build the repo
https://github.com/miyurud/EmailProcessor_Siddhi

Download the enron data set from the following command
wget https://www.dropbox.com/s/rf58pweo9jhl9h3/enron.avro?dl=0


Download and build the repo
Group ID is currently set thinking partition size as 3, change it to no of partitions relevant to the experiment

https://github.com/WinmaH/EnronEmailProcessor

Copy and paste the above downloaded data set inside the enronprocessor

Change the config.properties file path of enronprocessor to enron data set

Convert the following jars to OSGI bundles copy and paste to sp/lib

https://drive.google.com/open?id=1GAKeSC6xPZ39sT6nvLrvwVt6RRWUdcw5
