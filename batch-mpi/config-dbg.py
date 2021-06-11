# -------------------------------------------------------------------------
#
# THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
# ----------------------------------------------------------------------------------
# The example companies, organizations, products, domain names,
# e-mail addresses, logos, people, places, and events depicted
# herein are fictitious. No association with any real company,
# organization, product, domain name, email address, logo, person,
# places, or events is intended or should be inferred.
# --------------------------------------------------------------------------

# Global constant variables (Azure Storage account/Batch details)

# import "config.py" in "python_quickstart_client.py "

_BATCH_ACCOUNT_NAME = 'dbgbatch'  # Your batch account name
_BATCH_ACCOUNT_KEY = '+KdOX0CM5j6SVqbVfUt03UqNdOhCOYNpgmWRJTkB1tDyNl3zOO+c8EzfYDPKZd8FNbmaWNaWf2GKyarkuvtd7A=='  # Your batch account key
_BATCH_ACCOUNT_URL = 'https://dbgbatch.eastus.batch.azure.com'  # Your batch account URL
_BATCH_ACCOUNT_CONECTION_STRING =  "DefaultEndpointsProtocol=https;AccountName=dbgbatchstorage;AccountKey=AltrMQlJMDDjZJu+XcWUCgBclqWLciClr859PpHPLpX9g3qYbnxhMfwB40DjyFgxxs4XidthO+O8R1OZ6CrY1Q==;EndpointSuffix=core.windows.net"
_STORAGE_ACCOUNT_NAME = 'dbgbatchstorage'  # Your storage account name
_STORAGE_ACCOUNT_KEY = 'AltrMQlJMDDjZJu+XcWUCgBclqWLciClr859PpHPLpX9g3qYbnxhMfwB40DjyFgxxs4XidthO+O8R1OZ6CrY1Q=='  # Your storage account key
_POOL_ID = 'newdbgPool'  # Your Pool ID
_POOL_NODE_COUNT = 3  # Pool node count
_POOL_VM_SIZE = 'Standard_A1_v2' #'STANDARD_A1_v2'  # VM Type/Size
_JOB_ID = 'dbgJob'  # Job ID
_STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file