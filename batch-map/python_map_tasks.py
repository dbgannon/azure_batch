#this is a modified version of the batch-python-quickstart from the batch examples.
#the primary change is that it uses a user-supplied C function which has been 
#uploaded to the application as myapp version 1.

from __future__ import print_function
import datetime
import io
import os
import sys
import time
import config
try:
    input = raw_input
except NameError:
    pass

import azure.storage.blob as azureblob
import azure.batch as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels

sys.path.append('.')
sys.path.append('..')


# Update the Batch and Storage account credential strings in config.py with values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.

def query_yes_no(question, default="yes"):
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')

from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
from azure.storage.blob import generate_container_sas
def upload_file_to_container(container_name, file_path):
    """
    Uploads a local file to an Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)


    print('Uploading file {} to container [{}]...'.format(file_path,
                                                          container_name))

    # Upload content to block blob
    with open(file_path, "rb") as data:
       blob_client.upload_blob(data, blob_type="BlockBlob")
       
    account_name =config._STORAGE_ACCOUNT_NAME
    account_key = config._STORAGE_ACCOUNT_KEY 
   

    def get_blob_sas(account_name,account_key, container_name, blob_name):
        sas_blob = generate_blob_sas(account_name=account_name, 
                                container_name=container_name,
                                blob_name=blob_name,
                                account_key=account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.now(timezone.utc) + timedelta(hours=4))
        return sas_blob

    sas_token = get_blob_sas(account_name,account_key, container_name, blob_name)
    sas_url = 'https://'+account_name+'.blob.core.windows.net/'+container_name+ \
                '/'+blob_name+'?'+sas_token
    
    print('blob=name=', blob_name, ' url=', sas_url)
    return batchmodels.ResourceFile(http_url=sas_url, file_path=blob_name)


def get_container_sas_token(container_name, blob_permissions):
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately.
    account_name =config._STORAGE_ACCOUNT_NAME
    account_key = config._STORAGE_ACCOUNT_KEY 
   

    container_sas_token = \
        generate_container_sas(account_name=account_name, 
                                container_name=container_name,
                                account_key=account_key,
                                permission=blob_permissions,
                expiry=datetime.now(timezone.utc) + timedelta(hours=2))

    return container_sas_token


def create_pool(batch_service_client, pool_id):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print('Creating pool [{}]...'.format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="Canonical",
                offer="UbuntuServer",
                sku="18.04-LTS",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 18.04"),
        vm_size=config._POOL_VM_SIZE,
        target_dedicated_nodes=config._POOL_NODE_COUNT
    )
    new_pool.application_package_references = [batch.models.ApplicationPackageReference(
            application_id = "myapp", version = '1')]
    batch_service_client.pool.add(new_pool)


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)


def add_task(batch_service_client, job_id, input_file, task_num):
    """
    Adds a single task with id Task_X  for task num of the form '_X'

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A list containing an input file.
    """

    print('Adding {} tasks to job [{}]...'.format(1, job_id))

    tasks = list()
    command = "/bin/bash -c \"$AZ_BATCH_APP_PACKAGE_myapp_1/myapp < {}\"". \
        format(input_file.file_path)
    tasks.append(batch.models.TaskAddParameter(
        id='Task{}'.format(task_num),
        command_line=command,
        resource_files=[input_file]
    )
    )
    #tasks.append(batch.models.ApplicationPackageReference(
    #        application_id = "myapp", version = '1'))
    batch_service_client.task.add_collection(job_id, tasks)
  

def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def print_task_output(batch_service_client, job_id, encoding=None):
    """Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_id: The id of the job with task output files to print.
    """

    print('Printing task output...')

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:

        node_id = batch_service_client.task.get(
            job_id, task.id).node_info.node_id
        print("Task: {}".format(task.id))
        print("Node: {}".format(node_id))

        stream = batch_service_client.file.get_from_task(
            job_id, task.id, config._STANDARD_OUT_FILE_NAME)

        file_text = _read_stream_as_string(
            stream,
            encoding)
        print("Standard output:")
        print(file_text)


def _read_stream_as_string(stream, encoding):
    """Read stream as string

    :param stream: input stream generator
    :param str encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    :rtype: str
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = 'utf-8'
        return output.getvalue().decode(encoding)
    finally:
        output.close()
    raise RuntimeError('could not write data to stream or decode bytes')
import random

if __name__ == '__main__':

    start_time = datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    connect_str = config._BATCH_ACCOUNT_CONECTION_STRING
    print(connect_str)
    # Create the BlobServiceClient object which will be used to create a container client
    from azure.storage.blob import BlobServiceClient

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.

    input_container_name = 'input'
    try:
        container_client = blob_service_client.create_container(input_container_name)
    except:
        container_client = blob_service_client.get_container_client(input_container_name)
    
    
    # The collection of data files that are to be processed by the tasks.
     
    input_file_paths = [os.path.join(sys.path[0], 'taskdata0.txt'),
                        os.path.join(sys.path[0], 'taskdata1.txt'),
                        os.path.join(sys.path[0], 'taskdata2.txt'),
                        os.path.join(sys.path[0], 'taskdata3.txt')]                  
    # Upload the data files.
    input_files = [
        upload_file_to_container(input_container_name, file_path)
        for file_path in input_file_paths]
    
    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(config._BATCH_ACCOUNT_NAME,
                                                  config._BATCH_ACCOUNT_KEY)

    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=config._BATCH_ACCOUNT_URL)

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        pool_id = config._POOL_ID+str(random.randint(0,1000))
        create_pool(batch_client,pool_id)

        # Create the job that will run the tasks.
        job_id = config._JOB_ID+str(random.randint(0,1000))
        create_job(batch_client, job_id, pool_id)

        # Add the tasks to the job.
        add_task(batch_client, job_id, input_files[0], "_1")
        add_task(batch_client, job_id, input_files[1], "_2")
        add_task(batch_client, job_id, input_files[2], "_3")
        add_task(batch_client, job_id, input_files[3], "_4")

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client,
                                   job_id,
                                   timedelta(minutes=30))

        print("  Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, job_id)

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    # Clean up storage resources
    print('Deleting container [{}]...'.format(input_container_name))
    blob_service_client.delete_container(input_container_name)

    # Print out some timing info
    end_time = datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # Clean up Batch resources (if the user so chooses).
    if query_yes_no('Delete job?') == 'yes':
        batch_client.job.delete(job_id)

    if query_yes_no('Delete pool?') == 'yes':
        batch_client.pool.delete(pool_id)

    print()
    input('Press ENTER to exit...')
