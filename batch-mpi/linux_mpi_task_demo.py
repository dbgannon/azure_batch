# linux_mpi_task_demo.py - Batch Python tutorial sample for multi-instance
# tasks in linux (OpenFoam application)
#
# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import print_function
import datetime
import os
import sys

try:
    input = raw_input
except NameError:
    pass
from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
from azure.storage.blob import generate_container_sas

import azure.batch._batch_service_client as batch
import azure.batch.models as batchmodels
import azure.batch as batch
import azure.batch.batch_auth as batch_auth
os.lseek
import config
import multi_task_helpers

sys.path.append('.')
import common.helpers  


# Update the Batch and Storage account credential strings below with the values
# unique to your accounts.  These are used when constructing connection strings
# for the Batch and Storage client objects.
#_BATCH_ACCOUNT_KEY = '' These are from config.py and are set below
#_BATCH_ACCOUNT_NAME = ''
#_BATCH_ACCOUNT_URL = ''

#_STORAGE_ACCOUNT_NAME = '' these are from config.py and are set in helpers.py
#_STORAGE_ACCOUNT_KEY = ''

_OS_NAME = 'linux'
_APP_NAME = 'experiment'
_POOL_ID = common.helpers.generate_unique_resource_name(
    'pool_{}_{}'.format(_OS_NAME, _APP_NAME))
_POOL_NODE_COUNT = 3
_POOL_VM_SIZE ='Standard_A1_v2'
_NODE_OS_PUBLISHER = 'OpenLogic'
_NODE_OS_OFFER = 'CentOS-HPC'
_NODE_OS_SKU = '7.4'
_JOB_ID = 'job-{}'.format(_POOL_ID)
_TASK_ID = common.helpers.generate_unique_resource_name(
    'task_{}_{}'.format(_OS_NAME, _APP_NAME))
_TASK_OUTPUT_FILE_PATH_ON_VM = '../std*.txt'
_TASK_OUTPUT_BLOB_NAME = 'stdout.txt'
_NUM_INSTANCES = _POOL_NODE_COUNT


if __name__ == '__main__':

    start_time = datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Create the blob service client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    connect_str = config._BATCH_ACCOUNT_CONECTION_STRING
    # Create the BlobServiceClient object which will be used to create a containers
    from azure.storage.blob import BlobServiceClient

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # Use the blob service client to create the containers in Azure Storage if they
    # don't yet exist.
    input_container_name = 'input'
    output_container_name = 'output'
    try:
        blob_service_client.create_container(input_container_name)
        blob_service_client.create_container(output_container_name)
    except:
        print('containers exist')

    # Obtain a shared access signature that provides write access to the output
    # container to which the tasks will upload their output.
    output_container_sas_token =common.helpers.get_container_sas_token(
            output_container_name, BlobSasPermissions(write=True))

    output_container_sas = 'https://{}.blob.core.windows.net/{}?{}'.format(
        config._STORAGE_ACCOUNT_NAME, output_container_name, output_container_sas_token)

    # The collection of common scripts/data files that are to be
    # used/processed by all subtasks (including primary) in a
    # multi-instance task.
    common_file_paths = [os.path.join(sys.path[0], './data/coordination-cmd'),
                        os.path.join(sys.path[0], './data/numbers2.txt'),
                        os.path.join(sys.path[0], './data/numbers.txt')]

    # Upload the data files.  they will go to the "root" directory on each node
    common_files = [
        common.helpers.upload_file_to_container(
            blob_service_client,input_container_name, file_path)
        for file_path in common_file_paths]
   
    coordination_cmdline = [
        '$AZ_BATCH_TASK_SHARED_DIR/coordination-cmd']

    # The collection of scripts/data files that are to be used/processed by
    # the task (used/processed by primary in a multiinstance task).
    input_file_paths = [os.path.join(sys.path[0], './data/application-cmd')]
    
    # Upload the script/data files to Azure Storage
    input_files = [
        common.helpers.upload_file_to_container(
            blob_service_client,input_container_name, file_path)
        for file_path in input_file_paths]
    
    
    # Main application command to execute multiinstance task on a group of
    # nodes, eg. MPI.  we want to pass two arguments to the c application.
    num_path = "../numbers.txt ../numbers2.txt"
    application_cmdline = [
        '$AZ_BATCH_TASK_WORKING_DIR/application-cmd {} {}'.format(_NUM_INSTANCES, num_path )]
    print(application_cmdline[0])


    # Create a Batch service client.  We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(config._BATCH_ACCOUNT_NAME,
                                                  config._BATCH_ACCOUNT_KEY)

    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=config._BATCH_ACCOUNT_URL)

    

    # Create the pool that will contain the compute nodes that will execute the
    # tasks. The resource files we pass in are used for configuring the pool's
    # start task, which is executed each time a node first joins the pool (or
    # is rebooted or re-imaged).
    
    #multi_task_helpers.create_pool_and_wait_for_vms(
    #    batch_client, _POOL_ID, 'OpenLogic', 'CentOS-HPC', '7.4',
    #    'Standard_A1_v2', _POOL_NODE_COUNT)


    multi_task_helpers.create_pool_and_wait_for_vms(
        batch_client, _POOL_ID, _NODE_OS_PUBLISHER, _NODE_OS_OFFER,
        _NODE_OS_SKU, _POOL_VM_SIZE, _POOL_NODE_COUNT) 

    # Create the job that will run the tasks.
    common.helpers.create_job(batch_client, _JOB_ID, _POOL_ID)

    # Add the tasks to the job.  We need to supply a container shared access
    # signature (SAS) token for the tasks so that they can upload their output
    # to Azure Storage.
    multi_task_helpers.add_task(
        batch_client, _JOB_ID, _TASK_ID, _NUM_INSTANCES,
        common.helpers.wrap_commands_in_shell(_OS_NAME, application_cmdline),
        input_files, batchmodels.ElevationLevel.non_admin,
        _TASK_OUTPUT_FILE_PATH_ON_VM, output_container_sas,
        common.helpers.wrap_commands_in_shell(_OS_NAME, coordination_cmdline),
        common_files)

    # Pause execution until task (and all subtasks for a multiinstance task)
    # reach Completed state.
    multi_task_helpers.wait_for_tasks_to_complete(
        batch_client, _JOB_ID, timedelta(minutes=120))

    print("Success! Task reached the 'Completed' state within the "
          "specified timeout period.")

    # Print out some timing info
    end_time = datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # Download the task output files from the output Storage container to a
    # local directory
    if common.helpers.query_yes_no(
            'Download results ?') == 'yes':
        common.helpers.download_blob_from_container(
            blob_service_client,
            output_container_name,
            _TASK_OUTPUT_BLOB_NAME,
            os.path.expanduser('~'))

    # Clean up storage resources
    if common.helpers.query_yes_no(
            'Delete containers?') == 'yes':
        print('Deleting containers...')
        blob_service_client.delete_container(input_container_name)
        blob_service_client.delete_container(output_container_name)

    # Clean up Batch resources (if the user so chooses).
    if common.helpers.query_yes_no(
            'Delete job?') == 'yes':
        batch_client.job.delete(_JOB_ID)

    if common.helpers.query_yes_no(
            'Delete pool?') == 'yes':
        batch_client.pool.delete(_POOL_ID)

    print()
    input('Press ENTER to exit...')
