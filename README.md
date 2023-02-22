# Data Pipeline
### 1. Overview  
This is the pipeline of **RADAR** that is a decisional information system. It uses data to detect threats as early as possible.

### 2. Architecture 
   Pipeline consists in 3 modules:
   * A **Source** module which takes data from different sources, put them in a Hadoop cluster and do some basic operations in the cluster.
   * A **Warehouse** module which takes cleaned structured and semi-structured data in the cluster and put them in data marts.
   * An **Analytics** module which takes data in the data marts and apply some basic statistics operations (count, average, etc.) and advanced statistics operations (machine learning, deep learning, etc.) on them. The goal is to enhance the decision taking step.

### 3. Environment Setup
   Before beginning,  you should update and upgrade all packages of your system by doing:
   ```
   sudo apt update && sudo apt upgrade -y
   ```
   #### 3.0. Setup Python  
   The latest versions of Ubuntu generally come with Python3.8+.
   If Python is not installed, do:
   ```
   sudo apt install software-properties-common -y
   ```
   ```
   sudo add-apt-repository ppa:deadsnakes/ppa
   ```
   ```
   sudo apt install python3.8
   ```
   If it is not working well,follow the instructions of this [link](https://computingforgeeks.com/how-to-install-python-on-ubuntu-linux-system/)  
   Install pip3 by doing:
   ```
   sudo apt-get install python3-pip
   ```
   Install all the Python dependencies of the project
   ```
   pip3 install -r requirements.txt
   ```

   #### 3.1. Setup PostgreSQL
   Install PostgreSQL by doing:
   ```
   sudo apt install postgresql postgresql-contrib -y
   ```
   Enable PostgreSQL by doing:
   ```
   systemctl enable postgressql.service; service postgresql start
   ```
   Verify PostgreSQL is running by doing:
   ```
   service postgresql status
   ```

   #### 3.2. Setting Hadoop
   To configure hadoop, you need to setup Java and HDFS
   Follow this [link](https://www.digitalocean.com/community/tutorials/how-to-install-hadoop-in-stand-alone-mode-on-ubuntu-20-04) to do that.

### 4. How to run
Coming soon