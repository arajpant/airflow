#Instructions

## Contents

- [Project Description](#project-description)
- [Installation](#installation)
- [Install package](#install-package)
- [Deploy Locally](#deploy-locally)
- [Create Html File](#create-html-file)
- [Basic Programming Stuff](#basic-programming-stuff)

## Project Description

Airflow is the Scheduler ETL tool that helps to extract the data from different source, load the data to defined database and transform the data as required by end user.

## Installation

There is dedicated shell script to install Mkdocs. Click to the terminal & enter command shown below.

```
$ cd dags
$ bash msodbc_setup.sh
$ source mkdocs_install.sh
```

Or you can to it manually as mentioned in [Install package](#install-package) section.

## Install package

- Activate the venv (virtual environment)

  ```
  $ cd [Path-of-the-project-location]/airflow/dags/
  $ python3 -m venv venv
  $ source venv/bin/activate


  ```

- copy command listed below to install the mkdocs packages:

```
$ pip install pipx mkdocs markdown-callouts markdown-exec mkdocs-autorefs  mkdocs-coverage mkdocs-gen-files  mkdocs-literate-nav mkdocs-material mkdocs-material-extensions mkdocs-section-index  mkdocstrings mkdocstrings-python duty toml
```

## Deploy Locally

Enter the following command to run the mkdocs in your local machine.
Following command is for one time only.

```
$ cd [Path-of-the-project-location]/airflow/dags/mkdocs
$ make setup
```

For every build or run enter following command.

```
$ cd [Path-of-the-project-location]/sdlc/mkdocs
$ make docs-serve
```

open the browser and check the [Code Reference](http://127.0.0.1:2222/) url.

## Create Html File

HTML is created with following command:

```
$ cd [Path-of-the-project-location]/sdlc/mkdocs
$ make docs
```

The html file will be created under `site` folder inside dags->mkdocs folder.
Our python file documentation can be seen under `reference` folder.

## Basic Programming Stuff

To see your python file well documented. Please provide the function or class a good description
in docstring format.
A documentation string (docstring) is a string that describes a module, function, class, or method definition. The docstring is a special attribute of the object (object.**doc**) and, for consistency, is surrounded by triple double quotes, i.e.:<br/>
""" This is the form of a docstring. <br/>
It can be spread over several lines. <br/>
""" <br/>
Please check the code snippet for the proper way of documenting the method or function.

```
// Code Snippet
def add(a, b):
    """Compute and return the sum of two numbers.
    Args:
        a (float): A number representing the first addend in the addition.
        b (float): A number representing the second addend in the addition.
    Returns:
        float: A number representing the arithmetic sum of `a` and `b`.
    """
    return float(a + b)
```
