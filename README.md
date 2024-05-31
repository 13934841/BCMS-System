Greetings,

This is the code for an implementation of a Brokerage Client Management System (BCMS).
The purpose of this system is to enable our client, and similar brokerage services, to store, manage, and alayse their client's account and trading data, so that they can better understand their activity and habits, and plan accordingly.

This is accomplished primarily with the following files:
- datasetup.py: Provides the necessary functions for accessing the server and database.
- dimension_classes.py: Sets up the dimension tables.
- main.py: Performs the ETL processes.
- app.py: Provides the API for logging in.

To use this code, simply download it, run main.py, wait for the ETL steps to be complete, and then run app.py.
You can then navigate to the login screen by following the link in the terminal (with '/login' after it).
By entering the correct login information (see StaffDim.csv) you can access the desired dashboard.
