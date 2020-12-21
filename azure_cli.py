"""
Author : Rajwinder Singh Walia
Description : various functions wrapped around az command line interface
"""

import subprocess
import json
from azure.core import exceptions
import pyodbc
import os

def bashprocess(command: str) -> dict:
    """wrapper around subprocess to execute shell commands and get the output"""

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    processoutput, error = process.communicate()

    return {
        'output': processoutput.decode().strip(),
        'returncode': process.returncode
    }

def checl_az_cli_exitence() -> bool:
    """Check is az program is installed in the linux box"""

    #Checking the az availability on the linux box
    processdict = bashprocess("command -v az")

    if processdict['returncode'] != 0:
        print("az command line program is not available in the linux box")
        return False
    else:
        print("az comand line tool is installed in {} location".format(processdict['output']))
        return True

def azlogin(appid, tenantid, password):
    """Log on to Azure using service principal"""

    azurelogincmd = "az login --service-principal " + "-u " + appid + " --tenant " + tenantid + " --password " + password
    processdict = bashprocess(azurelogincmd)

    return json.loads(processdict['output'])[0]

def createstorageaccount(storagekind, resourcegroup, location, storageaccountname):
    """Create storage account under a resource group"""

    storageacctcmd = "az storage account create " + "--kind " + storagekind + " --name " + storageaccountname + " --location " + location +\
     " --resource-group " + resourcegroup
    processdict = bashprocess(storageacctcmd)

    return json.loads(processdict['output'])

def getstorageaccounturl(storageaccountname, resourcegroup, storagetype):
    """Get the service url for specific storage account under a resource group for a particular storage type"""

    storagetypeset = ('blob', 'file', 'queue', 'table')
    if storagetype not in storagetypeset:
        raise exceptions.AzureError("Storage Type passed is not correct. It should be one of {}".format(storagetypeset))

    acturlcmd = "az storage account show " + "--name " + storageaccountname + " --resource-group " + resourcegroup + " --query " + "primaryEndpoints." + storagetype
    processdict = bashprocess(acturlcmd)

    return processdict['output']

def getstorageaccountconnectionstring(storageaccountname, resourcegroup):
    """Get the connection string for the storage account which is needed as a credential to create a blob service client"""

    constrcmd = "az storage account show-connection-string" + " --name " + storageaccountname + " --resource-group " + resourcegroup
    processdict = bashprocess(constrcmd)

    return json.loads(processdict['output'])['connectionString']

def getsqldatabaseconnectionstring(databasename : str , client : str, servername : str ) -> str:
    """Get the connection string for the sql database to be used with client tool sqlcmd"""

    # Client tool validation
    clienttoolsallowed = ['sqlcmd','jdbc','odbc']
    if client not in clienttoolsallowed:
        raise exceptions.AzureError("Client provided is incorrect. Please provide one of {}".format(clienttoolsallowed))

    connstr = "az sql db show-connection-string" + " --client " + client + " --name " + databasename + " --server " + servername
    processdict = bashprocess(connstr)

    return processdict['output']

def getSqldatabaseODBCconn(databasename : str, servername : str) -> pyodbc.connect:
    """Get ODBC DB API connection to Azure SQL Database"""

    odbcconnstring = getsqldatabaseconnectionstring(databasename=databasename, client='odbc', servername=servername)
    
    """Get the different properties from the connection string by converting it into a dictionary"""
    connectionproperties = {prop[0] : prop[1] for prop in [connproperty.split('=') for connproperty in odbcconnstring.strip('"').rstrip(';').split(';')]}

    """Use server and database from the connection properties derived from the connection string as mentioned above"""    
    try:
        odbcconnection = pyodbc.connect("""
                                                                        Driver=SQL Server;
                                                                        Server={0};
                                                                        Database={1};
                                                                        Uid=XXXXXXXX@XXXX.com@XXXXXXX;
                                                                        Pwd=XXXXXXXX;
                                                                        Encrypt=yes;
                                                                        TrustServerCertificate=no;
                                                                        Connection Timeout=30;
                                                                        """.format(connectionproperties['Server'], connectionproperties['Database']))
    except Exception as e:
        raise pyodbc.Error(e)
    else:
        return odbcconnection
 
def getstorageaccountkeys(storageaccountname, resourcegroup):
    """Get both access keys for the storage account"""

    constrcmd = "az storage account keys list" + " --account-name " + storageaccountname + " --resource-group " + resourcegroup
    processdict = bashprocess(constrcmd)

    return processdict['output']