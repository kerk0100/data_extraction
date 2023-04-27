This is an MVP for the integration of Nanaimo open data with the UrbanLogiq platform. The dataset used to design 
the MVP was 'Business Licences', but the product is reusable on many of the other
datasets in the open data platform. 

**Task 1 Output:** [BusinessLicences.csv](BusinessLicences.csv) and the top 5 rows are saved to [solution.csv](solution.csv).

**Task 2 Output:** [BusinessLicences.parquet](BusinessLicences.parquet)

**Task 3 Output:** Difference in memory usage for the Business Licences dataset is:
* Memory usage before reduction: 1017 KB 
* Memory usage after reduction: 840 KB 
* Compressed File Size: 468 KB
* The stats are also printed out when the program is run.
* A compressed version is saved to [compressed.csv](compressed.csv) - in qzip format

**Task 4:** I tried the following datasets successfully:
* http://api.nanaimo.ca/dataservice/v1/sql/Construction/ (line 155 in [main.py](main.py))
* http://api.nanaimo.ca/dataservice/v1/sql/DevelopmentApplications/ (line 156 in [main.py](main.py))

To run the program:
* Install requirements: `````$ pip install -r requirements.txt`````
* From command line: `````$ python main.py [URL] [primary key]`````
  * If you do not include arguments when running the program, it will give you prompts to add the input within the program
* For the Business Licences dataset the command is: 
  * `````$ python main.py https://api.nanaimo.ca/dataservice/v1/sql/BusinessLicences/ licence`````

Assumptions/Process
* To better filter/clean the data, I have included a primary key as one of the arguments. This ensures for example that a business licence number isn't duplicated, and minimizes error in querying later on
* I assume that the date column in the original dataset gives the ability to detect its timezone
* I have used python 3.9
* I have included a few test cases (not exhaustive list) in [test.py](test.py)

