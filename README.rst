Migration Script from csv to PostgreSQL:
=========================================


** General Info:
    • Script took a max of **3 minutes** to migrate **20 million** records
    • (Python 3.11.6)
    • **ubuntu 23.10**
    • **16GB memory**
    • **20 million records in csv**
    • only one package is installed: **psycopg2==2.9.9**
    • Executed through Pycharm config file

How script is done:
    • Load the csv file in **generators in chunks**
    • Each chunk is **2000 rows**
    • 2000 rows are **split upon 2 threads**
    • Insert in the Database in **Bulk insert**

Project Setup:
    • create venv:
        ::

            virtualenv venv --python=3.11
            source venv/bin/activate

    * Add the **keys.json** file that was attached in the email in the **settings directory**

    • Install requirements:
        ::

            pip install -r requirements.txt

    • optional: I included a csv generator script, at the bottom you can decide how many rows to create (run it normally, no special params)
    * **Before you run**: if you have your own generator script, make sure column names in csv match the following:
        ::

            first name,last name,address,age

    • Migration script:
        ::

            python migrate.py dev

