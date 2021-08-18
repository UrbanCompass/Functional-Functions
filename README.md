# Functional-Functions
Created by the FBI Compass team, this package contains our team's most commonly used and useful functions

# About
Contents include functions for connecting to snowflake, saving and loading pickles. For help with what functions are available, please run help()

# Installation
To install functional-functions, you can install it using pip:
````python
pip install functional_functions
````

# Additional Prequisites
To use the redshift query connector, you must additionally ensure you have downloaded/installed:
* Redshift JDBC Driver (Mandatory)
    * You can usually install that from here: [Amazon JDBC Driver 2.0](https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-download-driver.html)
* Java (Optional)
    * You can usually install that from here: [JDK 16 Downloads](https://www.oracle.com/java/technologies/javase-jdk16-downloads.html)
* JDK8 (Optional)
    * For some reason, you may need this as well? This was troubleshooting I went through for setup
    * ````python
    brew tap adoptopenjdk/openjdk
    brew install --cask adoptopenjdk8
    ````


# Usage
Currently all files are in the init.py. Import each function as needed. First time users are encouraged to import and run help() for more info.

````python
import functional_functions
````

````python
from functional_functions import help, query_snowflake
````

# Troubleshooting/Prequisite/Additional Notes
Some functions, especially the connections, are built off of a settings.py file which stores local creds. You can view a sample settings file called 'settings.py.sample' in the included files. Obviously feel free to use another method to store and provide creds if you want.

However if you are using 'settings.py' as your creds file, you will need to place it in your overall site-packages folder.

**NOTE**: There is currrently an issue where an additional functional_functions.py file will appear in the package. Please disregard that file for now as the actual code is stored in __init__.py. 

**NOTE**: Yes I know code shouldnt be stored in __init__.py, but I also am learning packages and such!.

# Help/Issues/Bugs
Please contact lawrence.chin@compass.com if there are any questions, bugs, or issues.