# Python Spark Example

## Summary

Example Python (3.5) Spark application. Code performs the following actions: -

1. Configure and connect to local Spark instance.
2. Load two JSON format files into Spark RDDs.
3. Define and apply a schema to the RDDs to create Spark DataFrames.
4. Create temporary SQL views of the data.
5. Perform join between two datasets and output results to console.

## Setup

Code is designed to be run in a conda virtual environment. 
To setup and configure: -

1. Run `conda env create` from project base directory to setup the conda virtual environment.
2. Code needs a local Spark installation to run against, environment variable `SPARK_HOME` should be set and point to this location.
3. Run the code via `python main.py`.

## Files

<table>
<tr>
	<th>Name</th>
	<th>Description</th>
</tr>
	<td>data</td>
	<td>directory containing employee and titles json datasets</td>
<tr>
</tr>
	<td>environment.yml</td>
	<td>conda virtual environment specification</td>
<tr>
</tr>
	<td>logging.json</td>
	<td>Logging configuration</td>
<tr>
</tr>
	<td>main.py</td>
	<td>Main Python code</td>
<tr>
</tr>
	<td>pyspark exmaple.ipynb</td>
	<td>Jupyter notebook containing same code example</td>
<tr>
</tr>
	<td>utils</td>
	<td>Utility modules</td>
<tr>
</table>


## Note

- virtualenv activation and deactivation is controlled automatically via `autoenv` command which
executes `.env` and `.env.leave` scripts. See [autoenv](https://github.com/kennethreitz/autoenv)
- Repo also contains a jupyter notebook containing the same code. Use `jupyter notebook` to start a notebook server.

## References

- [apache spark](https://spark.apache.org/docs/2.1.1/sql-programming-guide.html#getting-started)
- [conda environemt workflow](https://tdhopper.com/conda/)
- [jupyter notebooks](http://jupyter.org/)
- [anaconda - miniconda](https://conda.io/miniconda.html)

## To Do

- Needs proper unittest suite.

Martin Robson 23/11/2017.
