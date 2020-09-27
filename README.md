# TrueFilm

## System Requirements
The following project should work as expected on UNIX Derived OSes, such as Linux and macOS, but it has only been tested on macOS.

In order to execute the program, only a bash script is provided, by understanding the content of such script users might be able to run this project on Windows as well.

## Architecture Overview

All the infrastructure to run the code or query the data is deployed using Docker containers.
This choice was made to guarantee that the project works seamlessly on every host OS supporting Docker, preventing anomalies caused by OS-related factors.

The following containers are used:
* Postgres, used as RDBMS where the output data is stored
* One spark-master container, used to process the input datasets and to publish the output data into Postgres. (Due to hardware limitations, it was not possible to simulate a Spark cluster by spawning also workern nodes.)
* PgAdmin, used to provide the user a UI to query the output data

All these containers and their configuration are defined in the `docker-compose.yml` file.

## Tools Overview

In order to implement the data processing, Apache Spark was the chosen framework.
Being around since six years and with a quite large user community, Apache Spark is one of the best tools to perform data processing, it allows to perform fast calculations by caching datasets directly in memory and by distributing the workload among a cluster of worker nodes, when cluster mode is enabled.  

Other than Apache Spark, it was necessary to use the following dependencies:
* spark-xml: an extension that provides Spark the capability to read and parse XML data sources.
* postgresql jdbc driver: JDBC Driver used to let Spark communicate with and send data to the target Postgres DB.

## Project Overview

The aim of this project is to provide insights about movies, with two datasets at disposal:

* **movies_metadata**: a dataset containing different informations about movies, taken from iMDB.com
* **enwiki-latest-abstract**: a dataset containing title, abstract, url and links inside each WikiPedia article.

In particular, users should be allowed to assess which genres and production companies are the top performing ones.
Moreover, users should be able to query the processed dataset in order to get some other insights of their choice.

## How To Run
### 1. Download and install Docker
Installing Docker is required to run this project. Installation guides and further informations can be found on [their website](https://www.docker.com/products/docker-desktop).

### 2. Download Input Datasets
In order to run the code, two files need to be placed inside the `input` folder:
* `enwiki-latest-abstract.xml`: The file is located inside the [gzip archive](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz) containing the latest Wikipedia English articles abstracts.
* `movies_metadata.csv`: The file can be downloaded by visiting [this](https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7#movies_metadata.csv) page on Kaggle.

### 3. Run The Program 

Once Docker is installed and the input datsets are placed inside the `input` folder, the program can be executed using the `truefilm-cli.sh` bash script.

First, let's start the containers by using
```shell script
./truefilm-cli.sh start
```

In order to check if the Docker containers are being started properly, the following command can be used in another terminal window to show logs
```shell script
./truefilm-cli.sh logs
```

Next, the actual data processing can be started with
```shell script
./truefilm-cli.sh process_data
```

Services can be accessed using the following links:
* **Spark UI:** http://localhost:4040
* **PgAdmin:** http://localhost:5050
    * username: pgadmin4@pgadmin.org
    * password: admin

**Important:** When accessing PgAdmin for the first time, it is necessary to setup a new connection to the postgres database.

Here are the connection details:
* **host:** postgres_container
* **username:** postgres
* **password:** changeme

Once the processing and data querying is completed, the Docker containers can be stopped with
```shell script
./truefilm-cli.sh stop
```

It is also possible to run all the unit tests by using
```shell script
./truefilm-cli.sh test
```

## Outputs Overview

The Spark code writes three tables to the Postgres database:

* movies(title, budget, year, revenue, rating, ratio, production_companies, url, abstract)
* top_genres(genre, sum_budget, sum_revenue, ratio)
* top_production_companies(production_company, sum_budget, sum_revenue, ratio)

In order to make the results coherent, `top_genres` and `top_prouction_companies` were produced using only the data contained inside the `movies` table.

## Algorithm Overview

The first step consists of parsing and cleaning both the data sources:

### 1. Processing of movies_metadata: 
After loading the CSV file, only the following columns are kept, and string columns are trimmed:
* title
* genres
* production_companies
* release_date
* budget
* revenue

The cleaning process consists of removing all the records that meet at least one of these criteria:
* Some Records are duplicated, therefore only one copy of them was kept
* Title null or empty
* Genres is null
* Production Companies is null
* Budget is null or negative
* Revenue is null or negative
* Both revenue and budget are zero
* Define a `ratio` column as the result of `budget/revenue`
* Define a `year` column by extracting it from `release_date`

The following transformations are then applied to the `title` column: 
* Remove from the `title` column any brackets and their contents (e.g. `Title (something)` becomes `Title`)
* Set `title` to lowercase

### 2. Processing of wikipedia_abstracts:
The XML file is loaded, schema inference is enabled, but with a `samplingRatio` of `0.01`, in order to speed up this process, since the XML file is well formed.
The following columns are kept and trimmed:
* title
* url
* abstract

The cleaning process consists of these steps:
* Remove from the `title` column the "Wikipedia: " prefix
* Remove from the `title` column any brackets and their contents (e.g. `Title (something)` becomes `Title`)
* Set `title` to lowercase
* Define a new column called `url_type` that contains the content extracted by braces in the `url` field, if present. Possible values are `1968_film`, `film`, `tv_series`, etc.

Some movie articles also contain the `Cast` anchor inside the `links.sublink.anchor` field, but since this does not happen in all the articles related to a movie, there is no preliminary filtering on the abstracts using this criteria.
An alternative to improve accuracy could have been to leverage the `abstract` field as well, by looking for keywords like 'screenplay', 'runtime', 'cast', 'plot', etc. but this field is not reliable as well.

### 3. Matching the two datasets
Since data sources are heterogeneous, record matching cannot be 1:1 and the matching criteria is based on some heuristics:

The record matching process is composed of these steps:
* First, `movies_metadata` and `wikipedia_abstracts` are joined using the cleaned version of the `title` column.
* Records from `movies_metadata` that match with exactly one Wikipedia abstract are considered matching records
* For records with more than one matching abstract, only records where `url_type` is set to `film` or `<movies_metadata.year>_film` are kept

### 4. Results and Improvements
After the deduplication and cleaning, the `movies_metadata` contains 10908 titles, this implementation is capable of matching 4167 of them with a record in the `wikipedia_abstract` dataset.

Unmatched records were removed from the dataset, as one of the requirements (easily browse the wikipedia URL) cannot be met.

Moreover, additional accuracy could have been obtained by using string metrics algorithms, such as Levensthein's distance, but at the cost of making the computation much heavier.

The `budget` and `revenue` columns in the `movies_metadata` dataset, sometimes are too low, probably because the unity of measurement is million dollars ($M) instead of dollars, but it would be almost impossible to determine a rule to distinguish movies that earned tens of thousand dollars from movies for which revenues are expressed in $M.