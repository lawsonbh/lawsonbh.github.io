Title: Load Local Files into Snowflake with Python
Date: 2022-10-10 08:14
Author: Bernie
Category: How-To


# Audience
You're going to love this article if you want to know more about:
- Snowflake Connector for Python (snowflake-connector-python)
- Storing configuration in the environment (python-decouple)
- Peeking at big files without totally opening them on your workstation (csv)
- Automating the boring stuff

Alternatively, you're going to love this article if you like providing feedback on any of the above!

If you want to follow along, then . . .
- . . . you should have a Snowflake workspace and understand the method you usually use to log into it. 
- . . . you should have git installed on your system.
- . . . have Python 3.9+. I used 3.10 on my personal workstation with my personal snowflake setup and 3.9 at my work. 


# Situation
I indirectly support five other engineers, a scrappy analyst, and a data scientist. All of them are trying to do the best they can within the constraints of our system / network connectivity. We work with third party organizations, on-prem systems, and a pretty dandy cloud data warehouse with accompanying AWS infrastructure on our end.


# Problem
I've encountered large (1 gb + uncompressed) files that need to be ingested from someone's workstation or a shared network file system. These types of adhoc movements are a minority of the requests but generate a large amount of pain for the team. 

There are quite a few of these floating around and while any individual file isn't necessarily re-occurring, the workflow is. Moreover, users sometimes have to load a dozen of these kinds of files sequentially because of the gui based tools on their workstation - leading to a large time sink of having to watch a query while sipping on coffee.

The goal of the user is to try and get the file to a sandbox area in one of our snowflake warehouses. 


# Solution
I wrote a python script to provide more automation with the potential for parallelization. I am setting up the script (WIP and feedback welcome!) to make it as easy as possible for users to re-use the functionality while keeping configuration strictly separate from the code. I wanted to enable users to do basic loads into the sandbox and take away the pain of hand coding DDLs when some of the files had hundreds of columns.


# Approach
## Step 1: Set up a local environment
Create a folder for your local data movement work and set up a virtual environment. When I wrote this, I worked on Ubuntu linux 22.04.01 LTS on my personal workstation and Ubuntu in Windows Subsytem for Linux (Ubuntu 22.04.01 LTS) at work, so the way you navigate system directories and manage your python environment may look a bit different if you are using a different flavor of operating system. I recommend [the official docs on venv](https://docs.python.org/3/tutorial/venv.html) for learning more. 

```bash
mkdir data-yeet
cd ./data-yeet
git init
python3 -m venv venv
source venv/bin/activate
```

Now that you are in your virtual environment, go ahead and pip install the following:

```bash
pip install --upgrade pip
pip install python-decouple snowflake-connector-python 
```

Begin strictly separating your configuration from your code by setting up your .env file. You can follow this template for a file named .env in ./data-yeet:

```bash
SNOWFLAKE_USER_NAME=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_AUTHENTICATOR=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DB=
SNOWFLAKE_SCHEMA=
LOCAL_FILE_PATH=
LOCAL_FILE_NAME=
TARGET_TABLE_NAME=
TRUNCATE=
CREATE=
``` 

Fill in the appropriate values for your snowflake workspace. For this type of workflow and given my company's SSO implementation, it made sense for set SNOWFLAKE_AUTHENTICATOR to externalbrowser. If you are running something with a personal setup, then adding in a field and value for SNOWFLAKE_PASSWORD instead may make sense. 

One of the things I love about the [Decouple package](https://pypi.org/project/python-decouple/) is because unlike using `os.environ` you can work with things other than strings, work with .ini OR .env files, and get really smooth, readable code that doesn't look like you had to implement workarounds. I really love it and kind of have a code-crush on it. 


## Step 2: Set up your SnowflakeConnection
In a file you create within ./data-yeet named `load_local_csvs_into_snowflake.py`, start with the following:
```python
from decouple import config
import snowflake.connector

snowflake_context = snowflake.connector.connect(
    user=config("SNOWFLAKE_USER_NAME"),
    account=config("SNOWFLAKE_ACCOUNT"),
    authenticator=config("SNOWFLAKE_AUTHENTICATOR","externalbrowser"), # You may choose instead to do a SNOWFLAKE_PASSWORD setting instead
)

snowflake_cursor = snowflake_context.cursor()

try:
    snowflake_cursor.execute("SELECT current_version()")
    one_row = snowflake_cursor.fetchone()
    print(one_row[0])
finally:
    snowflake_cursor.close()
snowflake_context.close()
```
Execute the above program with something like `python load_local_csvs_into_snowflake.py` and you should receive a single line of output to the terminal. Something like: `6.0.0`. The above example to test your connection was inspired by the [excellent documentation](https://docs.snowflake.com/en/user-guide/python-connector-install.html#step-2-verify-your-installation) by Snowflake themselves.


## Step 3: Write code to get the header row from a CSV
Ok - so the files I've been seeing the users struggle with are larger ones either by the file size or because of the number of columns. One of the things I wanted to unburden the users from was manually writing DDL statements with hundreds of columns or having an underpowered workstation freeze from trying to open an entire large file. 

The following code meets those needs quite nicely without too many additional dependencies. Let's take a look:
```python
##TODO: Use pathlib here
full_path_and_file = config("LOCAL_FILE_PATH") + config("LOCAL_FILE_NAME")
with open(full_path_and_file, "r", newline="") as file:
    reader = csv.reader(file, delimiter=",")
    header_row = next(reader)
```
We are using the csv module from the standard library to do the bulk of the work. First I hack together the full path and file name the csv module needs. In the future, we should definitely use pathlib to do this or just have a list of full paths supplied in the config. Once we supply the full path and file name in our `with open()` we can use the [generator.__next__() method](https://docs.python.org/3/reference/expressions.html#generator.__next__) on reader to only look at the first row of data without accessing the rest of the file. The row is stored in the reference `header_row` and allows us to iterate on its elements in other parts of our script. This does assume that the first row in the file of the csv is a header row, which is a disadvantage of this approach.

I liked this approach even though I consider myself a Pandas warrior. In Pandas, you could use the read_csv() method, but the reading of the file into the dataframe format, the additional parameters and code required to prevent a full read, and then the need to translate that back into a data type to supply for a sql DDL seemed overly excessive. 


## Step 4: Parameterized Create Table Statement
In a future post, I want to talk about doing this same thing but with [Jinja templating](https://pypi.org/project/Jinja2/). It feels like that should be the more appropriate tool for this use case, but I am not as comfortable implementing that in this kind of ad hoc workflow although I look forward to leveling up those Jinja skills in the future. Until then, the following works:
```python
def assemble_create_table_stmt(
    list_of_cols: list,
    target_table_name: str,
    database: str,
    schema: str,
):
    """
    Broke out the logic for assembling the SQL statement to create tables that don't exist
    """
    ##TODO: This feels like something we should do with jinja
    create_tbl_statement = f"""
    CREATE OR REPLACE TABLE {database}.{schema}.{target_table_name} (\n
    """
    ##TODO: Figure out data types in a better way
    for column_name in list_of_cols:
        create_tbl_statement = create_tbl_statement + column_name + " varchar(16777216)"
        if column_name != list_of_cols[-1]:
            create_tbl_statement = create_tbl_statement + ",\n"
        else:
            create_tbl_statement = create_tbl_statement + ")"
    return create_tbl_statement
```
In the above code, a couple of things are happening.
1. The Create Table sql has its database, schema, and target_table_name values parameterized base on the configuation you supply in your .env file. 
2. The header_row from our csv parsing code can be fed directly as an input into the `list_of_cols` argument, which then allows us to iterate through the elements of header_row and incrementally build out the rest of our sql statement as long as it isn't the last element as evaluated by `if column_name != list_of_cols[-1]:`
3. Once we arrive at the last element, we close our sql statement with the correct syntax - E.G. close parentheses instead of comma.
4. We then can return the sql statement string to whereever it's been called from.

Here's what I haven't figured out yet - data types in this thing. Right now if I let this script run loose through our teams, we are going to end up with some big tables in our sandbox. Ideally, I'll add in additional logic and configuration to allow a user to specify types if needed or to look at a second, third, or fourth row from the csv and infer types. I wanted to be mindful of the wide files with many columns and any iteration on this function I create I will keep those wide files with hundreds of fields top of mind.


## Step 5: Tying it all together
To tie the entire script together, I use the Snowflake cursor to execute the create table statement from the `assemble_create_table_stmt` function, a parameterized [put command](https://docs.snowflake.com/en/sql-reference/sql/put.html), and a parameterized [copy command](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html). I also allow the user to supply a True False bool to optionally truncate a table in the sandbox in case they need to reload data for whatever reason. I do require that the user intentionally set the bool value for create to True. I want to make sure they are modifying that config file. 

If the script is executed as `python load_local_csvs_into_snowflake.py`, then the `if __name__=="__main__":` block will take care of setting up the SnowflakeConnection from the config and executing the upload_csv_to_snowflake function with the appropriate parameterized inputs. Otherwise, these functions can be imported into another script created by the user to accelerate their work. Future work I do, I'd like to have one of the configs be dictionary that connects local files to load with their file paths. 

```python
import csv
from decouple import config
import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector import cursor


def assemble_create_table_stmt(
    list_of_cols: list,
    target_table_name: str,
    database: str,
    schema: str,

):
    """
    Broke out the logic for assembling the SQL statement to create tables that don't exist
    """
    ##TODO: This feels like something we should do with jinja
    ##TODO: Figure out feeding in database, schema
    create_tbl_statement = f"""
    CREATE OR REPLACE TABLE {database}.{schema}.{target_table_name} (\n
    """
    ##TODO: Figure out data types in a better way
    for column_name in list_of_cols:
        create_tbl_statement = create_tbl_statement + column_name + " varchar(16777216)"
        if column_name != list_of_cols[-1]:
            create_tbl_statement = create_tbl_statement + ",\n"
        else:
            create_tbl_statement = create_tbl_statement + ")"
    return create_tbl_statement


def upload_csv_to_snowflake(
    snowflake_context: SnowflakeConnection,
    local_file_name: str,
    target_table_name: str,
    local_file_path: str = "",
    truncate: bool = False,
    create: bool = False,
):
    """
    Take a snowflake context, a local csv file name, a file path to the csv
    and the name of the table you want to create or modify in snowflake
    Load that file at that path into snowflake
    """
    cs = ctx.cursor()
    ##TODO: Use pathlib here
    full_path_and_file = str(local_file_path) + str(local_file_name)
    try:
        if truncate:
            cs.execute(f"truncate table {target_table_name}")     
        if create:
            # If creating a new table, we can pull the field names from the
            # first row of the csv
            # If the csv doesn't have a header row it probably shouldn't be
            # loaded using this script or you would be loading to a table that
            # already exists
            with open(full_path_and_file, "r", newline="") as file:
                reader = csv.reader(file, delimiter=",")
                header_row = next(reader)
            create_table_stmt = assemble_create_table_stmt(
                list_of_cols=header_row, target_table_name=target_table_name
            )
            cs.execute(create_table_stmt)
        cs.execute(f"put file://{full_path_and_file}* @%{target_table_name}")
        cs.execute(f"copy into {target_table_name}")
    finally:
        cs.close()


if __name__ == "__main__":
    ctx = snowflake.connector.connect(
        user=config("SNOWFLAKE_USER_NAME"),
        account=config("SNOWFLAKE_ACCOUNT"),
        authenticator=config("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"), #default to externalbrowser if no config value
        role=config("SNOWFLAKE_ROLE"),
        warehouse=config("SNOWFLAKE_WAREHOUSE"),
        database=config("SNOWFLAKE_DB"),
        schema=config("SNOWFLAKE_SCHEMA"),
    )
    ## TODO: Take a list of local files to load
    ## TODO: Take a list of target table names
    ## TODO: Implement as dict operation
    upload_csv_to_snowflake(
        snowflake_context=ctx,
        local_file_name=config("LOCAL_FILE_TO_LOAD"),
        target_table_name=config("TARGET_TABLE_NAME"),
        local_file_path=config("PATH_TO_LOCAL_FILE",""), #defaults to blank if no config value
        create=True,
    )
    ctx.close()
```


# Future Steps
1. Work on condensing my blog posts - please let me know what is / is not clear : )
2. Use pathlib instead of hacking together file paths and names - will make it more portable across OS as well
3. Evaluate and implement better ways to supply / infer data types for the create table statement
4. Understand if Jinja will make building the DDL easier or harder for this workflow
5. Evaluate file compression and Snowflake stages before load into warehouse


# Thank you!
I really value that you read through to end. Again, please shoot me a note if you found this helpful and any suggestions you might have.