# Analysing Developer Contributions using Artifact Traceability Graphs

Software artifacts are like the by-products of the development process. Throughout the life cycle of a project, developers produce different artifacts such as source files and bug reports. To analyze developer contributions, we construct artifact traceability graphs with these artifacts and their relations using the data from software development and collaboration tools.

Developers are the main resource to build and maintain software projects. Since they keep the knowledge of the projects, developer turnover is a critical risk for software projects. From different viewpoints, some developers can be valuable and indispensable for the project. They are the key developers of the project, and identifying them is a crucial task for managerial decisions. Regardless of whether they are key developers or not, when any developer leaves the project, the work should be transferred to another developer. Even though all developers continue to work on the project, the knowledge distribution can be imbalanced among developers. Evaluating knowledge distribution is important since it might be an early warning for future problems.

We employ algorithms on artifact traceability graphs to identify key developers, recommend replacements for leaving developers and evaluate knowledge distribution among developers. We conduct experiments on six open source projects: Hadoop, Hive, Pig, HBase, Derby and Zookeeper. Then, we demonstrate that the identified key developers match the top commenters up to 98%, recommended replacements are correct up to 91% and identified knowledge distribution labels are compatible up to 94% with the baseline approach.

Previous works are published at **ESEC/FSE '19** [[1]](#1) and **PROMISE '20**[[2]](#2).

  

## `src` Files

 
-  [preprocess.py](preprocess.py): Reads data from sqlite3 databases, runs preprocessing steps, and generates a JSON file for each dataset.
-  [data_manager.py](data_manager.py): Responsible for reading data from JSON file and controling the sliding window mechanism.
-  [graph.py](graph.py): Creates artifact graph and developer graph by using the sliding window mechanism provided by `data_manager.py`, also identifies key developers with these graphs.
-  [main.py](main.py): Runs all 6 experiments in parallel and dumps results into pickle files.
-  [util.py](util.py): Includes a group of functions used by different scripts.
-  [extract_commenters.py](extract_commenters.py): Extracts commenters and their comment counts for each slidling window.
-  [extract_committers.py](extract_committers.py): Extracts committers and their commit counts for each slidling window.
-  [rq1_validation.py](rq1_validation.py): Generates top-k accuracy tables for key developer (RQ1).
-  [rq2_validation.py](rq2_validation.py): Generates topk-k accuracy and  MRR values for replacement validation (RQ2).
-  [rq3_validation.py](rq3_validation.py): Generates accuracy values for balanced and hero team comparing to one of the previous articles (RQ3).
-  [scalability.py](scalability.py): Runs key developer algorithms for all 6 projects, and measures average run time, average number of nodes and average number of edges for each project.
-  [statistics.py](scalability.py): Generate some statistical tables in the article such as average number of developers in the projects and dataset details.

## Start

Clone the repo.

`git clone https://github.com/hacetin/keydev.git`

Then, change the directory to the project folder.

`cd keydev`

## Install required packages
Using a virtual environment is recommended while installing the packages.

Python version is "3.9.1" in our experiments. `graph.py` uses "networkx 2.5" for graph operations and "scipy 1.5.4" for normality test. `main.py` uses "joblib 0.17.0" for parallel processing of the experiments.

You can install them seperately or use the following command to install the correct versions of all required packages.

`pip install -r requirements.txt`

## Reproduce results

### Preprocess
Generate JSON files.
1) Download Hadoop, Hive, Pig, HBase, Derby and Zookeeper datasets [[3]](#3) from [https://bit.ly/2wukCHc](https://bit.ly/2wukCHc), and extract the following files into **data** folder:
- hadoop.sqlite3
- hive.sqlite3
- pig.sqlite3
- hbase.sqlite3
- derby.sqlite3
- zookeeper.sqlite3

2) Run the preprocess script to generate JSON files for all 6 projects (takes a few seconds per project):

`python src\preprocess.py`

### Run experiments
Run 12 experiments (2 different sliding window sizes for each project) in parallel with the default configurations given in `main.py` (same configurations given in the article).

`python src\main.py`

This step can take hours depending on your system. It will create a pickle file for each experiment under **results** folder to keep the key developer for each day. You can see the logs under **logs** folder.

### Run validation script
Run corresponding validation script for each RQ to generate the results shared in the article.

`python src\rq1_validation.py`

`python src\rq2_validation.py`

`python src\rq3_validation.py`

### Extracting Statistics in the Tables
Run the following to get scalability statistics of key developer identificaition algortihms when they called all together. This step can take hours depending on your system:

`python src\scalability.py`

For number of leaving developers, dataset details after preprocessing and average number of developers, run the following script:

`python src\statistics.py`

## Run tests for `graph.py`, `data_manager.py` and `util.py`

By using a sample graph (data/test_data), we implemented unit tests for `graph.py` and `data_manager.py`. Also, we implemented tests for the functions in `util.py`.

Each script has own tests inside it. To run these tests, you can call them separately.

`python src\graph.py`

`python src\data_manager.py`

`python src\util.py`

You can inspect the sample graph step by step in [data/test_data/sample_graph_steps.pdf](data/test_data/sample_graph_steps.pdf).

## Web Tool

We also provide a proof of concept tool under "webtool" folder. This tool uses the pickle files in the "results" folder. Before using it, you must run the experiments. How to run the tool:

1. Install dash and its dependencies (using virtual environment is recommended):
`pip install dash==1.18.1`
2. Run the app:
`python app.py`
3. The app will be running on `http://127.0.0.1:8050/`

PS: Installing dash 1.18.1 should install its dependencies. In case you have any problem, try to install from "webtool\requirements.txt":
`pip install -r webtool\requirements.txt`

## References


<a  id="1">[1]</a> H. Alperen Cetin. 2019. Identifying the most valuable developers using artifact traceability graphs. In _Proceedings of the 2019 27th ACM Joint Meeting on European Software Engineering Conference and Symposium on the Foundations of Software Engineering_ (_ESEC/FSE 2019_). Association for Computing Machinery, New York, NY, USA, 1196–1198. DOI:https://doi.org/10.1145/3338906.3342487

<a  id="2">[2]</a> H. Alperen Çetin and Eray Tüzün. 2020. Identifying Key Developers using Artifact Traceability Graphs. In Proceedings of the 16th ACM International Conference on Predictive Models and Data Analytics in Software Engineering (PROMISE ’20), November 8–9, 2020, Virtual, USA. ACM, New York, NY, USA, 10 pages. https://doi.org/10.1145/3416508.3417116

Link to the article: [https://www.researchgate.net/publication/343712903_Identifying_Key_Developers_using_Artifact_Traceability_Graphs](https://www.researchgate.net/publication/343712903_Identifying_Key_Developers_using_Artifact_Traceability_Graphs)

<a  id="3">[3]</a> Michael Rath and Patrick Mäder. 2019. The SEOSS 33 dataset—Requirements, bug reports, code history, and trace links for entire projects. Data in brief 25 (2019), 104005.
