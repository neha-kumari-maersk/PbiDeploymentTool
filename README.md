# PbiTools
## _Tools to automate Pbi Reports and Datasets Tasks_

[![N|Solid](https://cldup.com/dTxpPi9lDf.thumb.png)](https://nodesource.com/products/nsolid)

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

## Features included in the application
- Create Datasets from the pbi template file
- Update Existing datasets in the power bi service to match the model in pbi template file
- Create reports (based on a published templatete report) connected to the multi tenant dataset
- Update the multi tenant reports content to match the published template report

The application will detect the published template report from your pbi template file and use that report as a template to clone/update the multi tenant reports. These Clonsed/Updated reports will be connected to the tenant dataset, following the pbi multi tenant architecture proposed by MSFT here:
[Msft Multi Tenant Documentation]

Some of the constraints implementing this type of architecture:
 - Adding a new customer
 - Updating a report for some or all customers
 - Updating the dataset schema for some or all customers
 - Frequency of dataset refreshes

The application accepts the following arguments:
| Argument | Description | Mandatory |
| ------ | ------ | ------ |
| u | client id to be used to connect both to XLMA endpoint and power bi rest api | true |
| p | client secret | true |
| tenantid | tenant id of the azure subscription | true |
| pbit | folder location fowr the unzipped pbit file content | true |
| env | environment variable that will be used to filter tenant_info.json file | true |
| reports-only | if the application should update only reports content, excluding any operations on the datasets | false (if not specified on the arguments will default to false, which means only datasets will be updated/created) |
| source | If specified will change the source of all the partitions in the dataset | false (if not specified will keep the existing source in the datasets) => If the application is running in reports-only mode this argument will be escaped as the datasets module will not be executed|

## Usage
The first step is to edit the tenant_info.json file first.

To execute the application use the following example code:
```sh
./ConsoleAppDatasetGenerator.exe u="********" p="*********" pbit="C:\Users\****\Documents\folder" tenantid="***********" env=prd source="Databricks.Catalogs(""adb-***************.2.azuredatabricks.net"", ""sql/protocolv1/o/***************/****-******-********"", [Database=null, EnableExperimentalFlagsV1_1_0=null])" reports-only=true
```

## License

See License File

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)

   [Msft Multi Tenant Documentation]: <https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-multi-tenancy?tabs=workspace>
