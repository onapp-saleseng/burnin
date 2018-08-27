# Requirements: 

- Python 2.7(CentOS 7), 
- `yum install MySQL-python` (The script should attempt to do this for you, not fully tested)
- An OnApp user with an API key generated. Admin(user id 1) is default, the -u flag can define a different user ID.
- Network connectivity to the virtual machine’s network from the control server(Attach CP temporarily to network or do some custom networking)



There is a more detailed description below on everything the script does, however it is designed to test a cloud’s hardware and configuration in a semi-real world situation. It creates VMs if necessary, and randomly performs actions on them to ensure they’re all working and possible.  

If your test has failed or you're just trying to run it longer after using the -k flag, you can just run the script as normal and it will detect the config file that it created, check for the batches output, and continue based off of the information here.  The list of test VMs is stored in this file as well, so the -z flag is not necessary when restarting and will override that list of VMs with ALL existing VMs.



## Multiple options to choose from:

* -b N, Batch mode(The only mode, only required to change the batch sizes). Provide a number here which will determine how much weight per batch. Jobs have weights, and are selected randomly until they fill the Batch Weight. Weights are only 1 or 2, so you could run as many jobs as this batch weight, or as little as half of the batch weight if they are bigger jobs.  Stop/Start VM is weight 1, while ResizeDisk is weight 2 for example. The default batch size is (NUM_VMS * 1.25), so that anywhere between 62.5 - 100% of the VMs will have actions every batch.
* -q and -v, quiet and verbose.  Verbose should only really be used when debugging something, quiet doesn’t do a whole ton right now.
* -w N, Workers. The amount of threads to be using while running batches of jobs, default is 8. The amount of transactions per hv/bs/datastore should be similar to this, I usually just use the amount of cores on the CP for this.
* -d, Defaults, Automatically just assume the defaults for the tests.
* -c, Continuous Mode, If there is a failure during the test then it will attempt to continue, until it reaches 25 failures which will end the program.
* -y, Yes mode, say Yes to everything.
* -p, Pretest mode, STILL TESTING,  This should run a test for N minutes before the batches test. 
* -z, This will try to run the test using the virtual machines that already exist.  This will fail with VMs in multiple zones right now.  This way you can also manually create a bunch of VMs and then test those instead of using this to automatically create them. This implies -k.  This also will OVERRIDE continuing from the VMs listed in the config file. 
* -k, Keep flag, this will NOT delete the VMs after the script is done if you use this.  This is implied by -z so that it won’t delete VMs that it didn’t create. You can extend the test this way.
* -g filename, Generate output, provide a filename. This will read a batch output file and process it to the normal JSON format. This is used when the script fails in the middle of the whole test but still has batches which you want the data for. NOT TESTED FULLY
* -t token, Submit data to the onapp architecture portal in order to view the data as HTML/PDF. Should output a link to the test.
* -u user, Define a user besides id 1 (admin) to use for the internal API calls. Can be useful if you want to limit specific things about the test.


If the test were to have failed previously, you should be able to run it again with the same flags and it will check for previous output and attempt to continue from there. You should be able to extend a test where the VMs were not deleted with this.

Once the test is started, the script will start gathering information.

- If there is more than one zone it will ask you to provide a Zone ID, printing the list of zones available.
- If there is more than one template available it will ask for a Template ID, printing the list of templates available.
- If there is more than one datastore available it will ask for a Datastore ID, printing the list of datastores available.
Then it will gather:
- VMs per HV
- Seconds to delay between batches
- Minutes the test should run for
- Whether or not you want to change the disk resize values or workload duration.
- - if yes: gathers disk resize amount, maximum disk size, and minutes to run disk workloads for
- - if no: defaults to 2GB increase, max 20GB, and workloads run 3 minutes.
- Whether or not you want to change the workload dd  parameters:
- - if yes: gathers seconds between workloads, the % of write workloads instead of read, the block size for dd, and the count for dd.
- - if no: Block size = 10M, Count = 10, Writes% = 0, Interval = 10 seconds
- VM Creation will begin here, and it will ask if you would like to customize VM resources:
- - if yes: gather Memory, CPU cores + shares, primary disk size
- - if no: cpu cores: 1, shares: 1, primary disk size: minimum disk size for template, memory: minimum memory for template.

It will attempt to copy the workload files to the virtual machines at this point, if it doesn’t detect it after two attempts then it will raise an error and quit. 

Then, the batches process will start, and end running when the latest batch has completed after the duration has elapsed:

- Makes sure all VMs are online.
- Generates a batch of jobs, randomly selecting VMs and then a Job for them until the sum of the jobs weight in each batch is equal to the batch weight limit. (More information on this below..)
- Run the batch in parallel, the -w, workers parameter controls the threads for this.
- Writes this data to the bottom of a batches.out file. 
- Logs the data, if verbose it also prints it. 
- Delays for the amount of time specified until restarting
- The batch ID and number of jobs will print out by default, using -v or —verbose will print out ALL of the job data and parameters, lots of output.
- The script will then process the data from the batches, gather IOPS data from each VM’s disk, and process these into a python dictionary set, similar to json. This will include the configuration data from the test.
- It will write the python data to test_results.pydat, and the JSON data to test_results.json.  
- Using the -t flag it will submit to the architecture portal with that token.
- Finally, it will unlock and delete all the virtual machines that it was using for the test, unless you specific -z or -k flags.

# More information on generating batches of jobs:

All jobs have a weight of 1 or 2: [('MigrateVM',1), ('EditDisk',2), ('CreateBackup',1), ('RestoreBackup',2), ('SingleWorkload',2), ('stopStartVM',1)]

The program will run a loop:

- A random job is chosen from that list, and if there is not enough weight left it reselects until it gets one.
- A random VM is chosen from a temporary list of test VMs, there will never be more jobs than VMs in the test.
- Depending on the job, it generates a few things and does some checks, then adds the job to a list.
- Repeat this behavior until there are no more VMs to give tests to or the weight limit has been reached.

