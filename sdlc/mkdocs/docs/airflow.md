#Instructions

## Contents

- [Project Description](#project-description)
- [Pre-requisites](#pre-requisites)
- [Installation](#installation)

> Tips: This installation guide will be more effective if you have Linux Operating System.

## Project Description

Airflow is the Scheduler ETL tool that helps to extract the data from different source, load the data to defined database and transform the data as required by end user.

## Pre-requisites

Before Airflow setup, there are some requirement that needs to be satisfied.

- Python 3.x
- Docker Desktop
- MSODBC

## Installation

You must install python on your machine first. You can install it using Python's [Official Website](https://www.python.org/downloads/source/).

Docker Desktop is required to be installed before installing Airflow. You can install Docker Desktop from their [Official Website](https://docs.docker.com/desktop/install/linux-install/).

> Note: You need to be signed in to the Docker Hub Account before configuring anything in Docker Desktop.

After installing docker desktop, install MSODBC with commands listed below:

```
$ cd dags
$ bash msodbc_setup.sh
```

or you can do it manually,

```
$ if ! [[ "18.04 20.04 22.04" == *"$(lsb_release -rs)"* ]];
    then
        echo "Ubuntu $(lsb_release -rs) is not currently supported.";
        exit;
    fi

$ sudo su
$ curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

$ curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list

$ exit
$ sudo apt-get update
$ sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
# optional: for bcp and sqlcmd
$ sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
$ echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
$ source ~/.bashrc
# optional: for unixODBC development headers
$ sudo apt-get install -y unixodbc-dev
```

To check if MSODBC is installed or not, following command can be used:

```
$ cd / &&  cat etc/odbcinst.ini
```

It will return list of the ODBC drivers with their versions. Check if There is MSODBC driver.

Finally, you can now setup Airflow. Firstly clone the project from Bitbucket and take the latest pull from Integration/Airflow branch.

Go to the root directory of airflow and enter following commands:

```
$ docker-compose build
```

This will create image of Airflow in Docker. You can verify it using Docker Desktop.

> You need to launch Docker Desktop before entering above command.

Now we will pull that Image to container using command shown below:

```
$ docker-compose up -d
```

This will sucessfully create container for Airflow which can be verified again, from Docker Desktop.

> -d flag denotes Demon mode of docker container which will run container in background.

There are no user for Airflow in initial installation of Airflow, so we need to create user.

- Open Docker Deskop and go to Containers.
- Click on Airflow_webserver_1 container listed there. You will see Terminal icon on top-right side (this may be different).
- Click to the terminal & enter command shown below.

```
$ cd dags
$ bash new_user.sh
```

This will ask for the user inputs for your email, name, username, password & role. Your login details will be appear if you fill all the inputs.

Airflow can now be opened from any browser using Airflow Domain (http://localhost:2020/).
Airflow dashboard will appear if installation is done correctly.
