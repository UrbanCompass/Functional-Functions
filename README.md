# Functional-Functions
Created by the FBI Compass team, this package contains our team's most commonly used and useful functions

# About
Contents include functions for connecting to snowflake, saving and loading pickles. For help with what functions are available, please run help()

# Usage
Currently all files are in the init.py. Import each function as needed. First time users are encouraged to import and run help() for more info.

````python
import functional_functions
````

````python
from functional_functions import help, query_snowflake
````

# Prequisite/Additional Note
Some functions, especially the connections, are built off of a settings.py file which stores local creds. You can view a sample settings file called 'settings.py.sample' at the github link. Obviously feel free to use another method to store and provide creds if you want.

# Help/Issues/Bugs
Please contact lawrence.chin@compass.com if there are any questions, bugs, or issues.