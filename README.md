# Automated Data Transfer from Azure Blob Storage to Azure SQL Database

This repository contains the code for an automated system that transfers CSV data from Azure Blob Storage to an Azure SQL Database. The project leverages Azure Data Factory and Logic Apps to automate the data transfer process, and a Python script running inside a Docker container to handle the data transfer tasks. The Docker container is stored in Azure Container Registry (ACR) and deployed with Azure Container Instances (ACI) for scalability. For data visualization and analysis, Power BI is used.

## Contact

script_name.py: The main Python script that sets up the necessary connections with Azure Blob Storage and Azure SQL Database, scans the Blob Storage container for blobs, generates table names based on the blobs' names, creates new tables in the SQL database if they don't exist, and uses Azure Data Factory to transfer data from the blobs to the newly created tables.

##Prerequisites

Before running this script, you need the necessary permissions and credentials for Azure Blob Storage and Azure SQL Database. These credentials should be updated in the Python script before running. The script also requires certain Python libraries, which are listed in the requirements.txt file. You can install these dependencies in your environment using pip:
Copy code
pip install -r requirements.txt

##Usage

To use this system, follow these steps:

Clone this repository to your local machine.
Update the script_name.py file with your Azure Blob Storage and Azure SQL Database credentials.
Run the script_name.py script:

Copy code
python script_name.py
This will start the data transfer process. The script provides status updates in the terminal.

Please note that this script is part of a larger project that also includes Docker, Logic Apps, and Power BI components. For more information on these components and how to use them, please refer to the project documentation.

Remember to replace script_name.py with the actual name of your Python script. Also, if you have more specific instructions or steps for running the project, include them in the Usage section.
This Python script is used to automate the transfer of CSV data from Azure Blob Storage to an Azure SQL Database.

## Contact
If you encounter any problems or have any questions about this project, feel free to reach out to me. You can contact me at <your-email-address>.

## Issues and Questions
If you encounter any problems or have any questions about this project, please open an issue or start a discussion. I'll do my best to respond as quickly as I can.
