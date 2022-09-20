using System;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System.Net.Http;
using System.Net.Http.Headers;
using Microsoft.AnalysisServices.Tabular;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace ConsoleAppDatasetGenerator
{
    internal class Program
    {
        private static Dictionary<string, string> _appArgs;
        static void Main(string[] args)
        {
            var argsDictionary = args.Select(a => a.Split(new[] { '=' }, 2))
                     .GroupBy(a => a[0].Trim(), a => a.Length == 2 ? a[1].Trim() : null)
                     .ToDictionary(g => g.Key.Trim(), g => g.FirstOrDefault());

            _appArgs = argsDictionary;

            string clientId = getArgValue("u", true);
            string clientSecret = getArgValue("p", true);
            string pbitPath = getArgValue("pbit", true);
            string tenantId = getArgValue("tenantid", true);
            string datasetSource = getArgValue("source");
            string environment = getArgValue("env", true);
            Console.WriteLine("Executing for environment: " + environment);
            string updateReportsOnlyArg = getArgValue("reports-only");
            updateReportsOnlyArg = String.IsNullOrEmpty(updateReportsOnlyArg) ? "false" : updateReportsOnlyArg;
            bool updateReportsOnly = Convert.ToBoolean(updateReportsOnlyArg);
            
            string tenantInfo = File.ReadAllText("./Configs/tenant_info.json");

            string pbitConnections = pbitPath + "\\Connections";
            string pbitDataModelSchema = pbitPath + "\\DataModelSchema";
            string modelName = pbitPath.Split('\\').Last().Split('.').First();
            Database templateDatabase = new Database();
            if (!updateReportsOnly)
            {
                string jsonTemplateModel = File.ReadAllText(pbitDataModelSchema, encoding: System.Text.Encoding.Unicode);
                templateDatabase = Microsoft.AnalysisServices.Tabular.JsonSerializer.DeserializeDatabase(jsonTemplateModel);
            }

            List<TenantEnvironmentsConfiguration> allTenants = JsonConvert.DeserializeObject<List<TenantEnvironmentsConfiguration>>(tenantInfo);
            allTenants = allTenants.Where(e => e.env == environment).ToList();
            if(allTenants.Count == 0)
            {
                throw new Exception("No tenant definition found for env: " + environment);
            }

            if (!Directory.Exists("./output/"))
            {
                Directory.CreateDirectory("./output");
            }

            PbiApiInfo apiInfo = new PbiApiInfo
            {
                tenantId=tenantId,
                clientId=clientId,
                clientSecret=clientSecret
            };

            ReplicateDatasets datasetsManager = new ReplicateDatasets(allTenants, templateDatabase, datasetSource, apiInfo, updateReportsOnly, modelName);
            datasetsManager.GenerateDatasets().GetAwaiter().GetResult();
        }


        private static string getArgValue(string key, bool mandatory = false)
        {
            string argValue = _appArgs.ContainsKey(key) ? _appArgs[key] : String.Empty;
            if (mandatory && String.IsNullOrEmpty(argValue))
            {
                throw new Exception("Mandatory argument doesn't have value: " + key);
            }
            return argValue.Trim();
        }

        private static void DeployReports()
        {

        }
    }

    class PbiApiInfo
    {
        public string tenantId { get; set; }
        public string clientId { get; set; }
        public string clientSecret { get; set; }
    }

    class RefreshRequest
    {
        public string type { get; set; }
        public int maxParallelism { get; set; }
    }

    class ReplicateDatasets
    {
        private List<TenantModel> _tenantInfo;
        private Database _templateDatabase;
        string _datasetSource;
        private PbiApiInfo _pbiApiInfo;
        private bool _updateReportsOnly;
        private string _modelName;

        public ReplicateDatasets(List<TenantEnvironmentsConfiguration> tenantInfo, Database templateDatabase, string datasetSource, PbiApiInfo pbiApiInfo, bool updateReportsOnly, string modelName)
        {
            _tenantInfo = tenantInfo.FirstOrDefault().tenants;
            _templateDatabase = templateDatabase;
            _datasetSource = datasetSource;
            _pbiApiInfo = pbiApiInfo;
            _updateReportsOnly = updateReportsOnly;
            _modelName = modelName;
        }

        public async Task GenerateDatasets()
        {
            PbiWrapper pbiApi = new PbiWrapper(_pbiApiInfo.tenantId, _pbiApiInfo.clientId, _pbiApiInfo.clientSecret);
            string groups = await pbiApi.GetGroups();
            dynamic workspaces = JsonConvert.DeserializeObject<dynamic>(groups).value;
            
            foreach (TenantModel tenant in _tenantInfo)
            {
                bool deployReports = _updateReportsOnly == true ? true : false;
                bool deployDatasets = _updateReportsOnly == false ? true : false;
                TenantDeployer deployer = new TenantDeployer(_pbiApiInfo, tenant, deployDatasets, deployReports, _templateDatabase, _modelName, _datasetSource);
                await deployer.Main();
            }
        }
    }

    class TenantDeployer
    {
        private TenantModel _tenantModel;
        private PbiApiInfo _pbiApiInfo;
        private bool _deployDatasets;
        private bool _deployReports;
        private Database _templateDatabase;
        private string _modelName;
        private string _datasetSource;

        public TenantDeployer(PbiApiInfo pbiApiInfo, TenantModel tenantInfo, bool deployDatasets, bool deployReports, Database templateDatabase, string modelName, string datasetSource)
        {
            _tenantModel = tenantInfo;
            _pbiApiInfo = pbiApiInfo;
            _deployDatasets = deployDatasets;
            _deployReports = deployReports;
            _templateDatabase = templateDatabase;
            _modelName = modelName;
            _datasetSource = datasetSource;
        }

        public async Task Main()
        {
            if (_deployDatasets)
            {
                DeployDatasets(_templateDatabase, _modelName, _datasetSource);
            }
            if (_deployReports)
            {
                await DeployReports();
            }
        }

        private void DeployDatasets(Database templateDatabase, string modelName, string datasetSource)
        {
            //Dataset Deployment is made through XMLA Workspace Endpoints
            string datasetWorkspaceName = _tenantModel.workspaceToDataset;
            string toConnectionString = "powerbi://api.powerbi.com/v1.0/myorg/" + datasetWorkspaceName;
            toConnectionString = $"DataSource={toConnectionString};User ID=app:{_pbiApiInfo.clientId}@{_pbiApiInfo.tenantId};Password={_pbiApiInfo.clientSecret};";
            Database templateDatabaseSrc = templateDatabase.Clone();
            int tenantId = _tenantModel.tenantId;
            string tenantCode = _tenantModel.tenantCode;
            string currentTenantDatasetId = String.Empty;
            string databaseName = $"{modelName}_" + tenantCode.ToLower();

            Console.WriteLine("Generating and Deploying the dataset for " + tenantCode);
            TenantDatasetGenerator generator = new TenantDatasetGenerator(tenantId: tenantId, templateDatabase: templateDatabaseSrc, datasetSource);
            string saveFileName = databaseName + ".bim";
            Database outDatabase = generator.SaveModelBim("./output/" + saveFileName);
            currentTenantDatasetId = TenantDatasetGenerator.DeployDatabase(toConnectionString, outDatabase, databaseName, tenantCode.ToLower());
        }

        private async Task DeployReports()
        {
            //Report Deployment is made through the PBI API
            PbiWrapper pbiApi = new PbiWrapper(_pbiApiInfo.tenantId, _pbiApiInfo.clientId, _pbiApiInfo.clientSecret);
            string datasetWorkspaceName = _tenantModel.workspaceToDataset;
            string datasetWorkspaceId = (JsonConvert.DeserializeObject<dynamic>(await pbiApi.GetGroupByName(datasetWorkspaceName)).value)[0].id;
            //get the distinct names of the datasets that the reports will be connected to
            //after that we will get the datasets id so we can publish the reports connected to the tenant datasets
            List<string> datasetNames = _tenantModel.reports.Select(r => r.connectToDataset).ToList();
            List<KeyValuePair<string, string>> datasets = new List<KeyValuePair<string, string>>();
            var datasetsWorkspace = JObject.Parse(await pbiApi.GetDatasetsInGroup(datasetWorkspaceId))["value"];
            foreach (var dataset in datasetsWorkspace)
            {
                string datasetId = (string)dataset["id"];
                string datasetName = (string)dataset["name"];
                datasets.Add(new KeyValuePair<string, string>(datasetId, datasetName));
            }
            
            //Deply the reports for each configured dataset
            foreach(string dataset in datasetNames)
            {
                //Get the current dataset id that we are deploying
                string currentDatasetId = datasets.Where(d => d.Value == dataset + "_" + _tenantModel.tenantCode.ToLower()).Select(d => d.Key).FirstOrDefault();
                foreach(ReportModel report in _tenantModel.reports.Where(r => r.connectToDataset == dataset))
                {
                    //Get current report template workspace id and report id as well as the target workspace id
                    string fromWorkspace = report.fromWorkspace;
                    string templateReportWorkspaceId = (string)(JObject.Parse(await pbiApi.GetGroupByName(fromWorkspace))["value"][0]["id"]);
                    var reportsInWorkspaceId = JObject.Parse(await pbiApi.GetReportsInGroup(templateReportWorkspaceId))["value"];
                    string templateReportId = (string)(reportsInWorkspaceId.Where(r => (string)r["name"] == report.reportName).FirstOrDefault())["id"];

                    //Check if the report already exists in the target workspace
                    string toWorkspace = report.toWorkspace;
                    string destinationReportName = report.reportName + " (" + _tenantModel.tenantCode.ToUpper() + ")";
                    string destinationReportWorkspaceId = (string)(JObject.Parse(await pbiApi.GetGroupByName(toWorkspace))["value"][0]["id"]);
                    var reportsInTargetWorkspaceId = JObject.Parse(await pbiApi.GetReportsInGroup(destinationReportWorkspaceId))["value"];
                    var destinationReport = (reportsInTargetWorkspaceId.Where(r => (string) r["name"] == destinationReportName).FirstOrDefault());
                    string result = String.Empty;
                    if (destinationReport == null)
                    {
                        Console.WriteLine("Creating report " + destinationReportName);
                        //Create report in destination workspace
                        result = await pbiApi.CloneReport(templateReportWorkspaceId, templateReportId, destinationReportName, currentDatasetId, destinationReportWorkspaceId);
                    }
                    else
                    {
                        Console.WriteLine("Updating report " + destinationReportName);
                        string destinationReportId = (string)destinationReport["id"];
                        //Update report in the destination
                        result = await pbiApi.UpdateReport(destinationReportWorkspaceId, destinationReportId, templateReportId, templateReportWorkspaceId);
                    }
                    Console.WriteLine(result);
                }
            }
        }
    }

    class PbiWrapper
    {
        private string _tenantId;
        private string _clientId;
        private string _clientSecret;
        public PbiWrapper(string tenantId, string clientId, string clientSecret)
        {
            _tenantId = tenantId;
            _clientId = clientId;
            _clientSecret = clientSecret;
        }

        public async Task<string> GetDatasetsInGroup(string workspaceId)
        {
            HttpClient client = new HttpClient();

            // Send refresh request
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());
            HttpResponseMessage response = await client.GetAsync($"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/datasets");
            string content = await response.Content.ReadAsStringAsync();
            return content;
        }

        public async Task<string> GetReportInGroup(string workspaceId, string reportId)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());
            HttpResponseMessage response = await client.GetAsync($"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/reports/{reportId}");
            string content = await response.Content.ReadAsStringAsync();
            return content;
        }

        public async Task<string> CloneReport(string workspaceId, string reportId, string newReportName, string targetModelId, string targetWorkspaceId)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());
            CloneReportRequest cloneData = new CloneReportRequest
            {
                name = newReportName,
                targetModelId = targetModelId,
                targetWorkspaceId = targetWorkspaceId
            };
            HttpResponseMessage response = await client.PostAsJsonAsync($"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/reports/{reportId}/Clone", cloneData);
            string content = await response.Content.ReadAsStringAsync();
            return content;
        }

        public async Task<string> UpdateReport(string workspaceId, string reportId, string sourceReportId, string sourceWorkspaceId)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());
            UpdateReportRequest updateData = new UpdateReportRequest
            {
                sourceReport = new SourceReportModel
                {
                    sourceReportId = sourceReportId,
                    sourceWorkspaceId = sourceWorkspaceId
                },
                sourceType = "ExistingReport"
            };
            HttpResponseMessage response = await client.PostAsJsonAsync($"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/reports/{reportId}/UpdateReportContent", updateData);
            string content = await response.Content.ReadAsStringAsync();
            return content;
        }

        public async Task<string> GetReportsInGroup(string workspaceId)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());
            HttpResponseMessage response = await client.GetAsync($"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/reports");
            string content = await response.Content.ReadAsStringAsync();
            return content;
        }

        public async Task<string> GetGroupByName(string workspaceName)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());
            HttpResponseMessage response = await client.GetAsync($"https://api.powerbi.com/v1.0/myorg/groups?%24filter=name%20eq%20%27{workspaceName}%27");
            string content = await response.Content.ReadAsStringAsync();
            return content;
        }

        public async Task<string> GetGroups()
        {
            HttpClient client = new HttpClient();

            // Send refresh request
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());
            HttpResponseMessage response = await client.GetAsync("https://api.powerbi.com/v1.0/myorg/groups");
            string content = await response.Content.ReadAsStringAsync();
            return content;
        }

        private async Task<string> UpdateToken()
        {

            // AAS REST API Inputs:
            // string resourceURI = "https://*.asazure.windows.net";
            // string authority = "https://login.windows.net/<TenantID>/oauth2/authorize";
            // AuthenticationContext ac = new AuthenticationContext(authority);

            // PBI REST API Inputs:
            string tenantId = _tenantId;
            string resourceURI = "https://analysis.windows.net/powerbi/api";
            string authority = $"https://login.microsoftonline.com/{tenantId}";
            string[] scopes = new string[] { $"{resourceURI}/.default" };



            #region Use Interactive or username/password

            //string clientID = "<App ID>"; // Native app with necessary API permissions

            //Interactive login if not cached:
            //AuthenticationContext ac = new AuthenticationContext(authority);
            //AuthenticationResult ar = await ac.AcquireTokenAsync(resourceURI, clientID, new Uri("urn:ietf:wg:oauth:2.0:oob"), new PlatformParameters(PromptBehavior.SelectAccount));

            // Username/password:
            // AuthenticationContext ac = new AuthenticationContext(authority);
            // UserPasswordCredential cred = new UserPasswordCredential("<User ID (UPN e-mail format)>", "<Password>");
            // AuthenticationResult ar = await ac.AcquireTokenAsync(resourceURI, clientID, cred);

            #endregion

            // AAS Service Principal:
            // ClientCredential cred = new ClientCredential("<App ID>", "<App Key>");
            // AuthenticationResult ar = await ac.AcquireTokenAsync(resourceURI, cred);


            // PBI Service Principal: 
            AuthenticationContext ac = new AuthenticationContext(authority);
            ClientCredential cred = new ClientCredential(_clientId, _clientSecret);
            AuthenticationResult ar = await ac.AcquireTokenAsync(resourceURI, cred);

            return ar.AccessToken;
        }
    }

    class CloneReportRequest
    {
        public string name { get; set; }
        public string targetModelId { get; set; }
        public string targetWorkspaceId { get; set; }
    }

    class UpdateReportRequest
    {
        public SourceReportModel sourceReport { get; set; }
        public string sourceType { get; set; }
    }

    class SourceReportModel
    {
        public string sourceReportId { get; set; }
        public string sourceWorkspaceId { get; set; }
    }

    class PbitConnectionsModel
    {
        public int Version { get; set; }
        public List<RemoteArtifactModel> RemoteArtifacts { get; set; }
    }

    class RemoteArtifactModel
    {
        public string DatasetId { get; set; }
        public string ReportId { get; set; }
    }

    class TenantEnvironmentsConfiguration
    {
        public string env { get; set; }
        public List<TenantModel> tenants { get; set; }
    }

    class TenantModel
    {
        public int tenantId { get; set; }
        public string tenantCode { get; set; }
        public string name { get; set; }
        public string toDatasetName { get; set; }
        public string workspaceToDataset { get; set; }
        public string workspaceFromReport { get; set; }
        public string workspaceToReport { get; set; }
        public List<ReportModel> reports { get; set; }
    }

    class ReportModel
    {
        public string reportName { get; set; }
        public string fromWorkspace { get; set; }
        public string toWorkspace { get; set; }
        public string connectToDataset { get; set; }
    }

    class PartitioningModel
    {
        public List<DatasetModel> datasets { get; set; }
    }

    class DatasetModel
    {
        public string dataset_name { get; set; }
        public List<DatasetDefinition> definition { get; set; }
    }

    class DatasetDefinition
    {
        public string table_name { get; set; }
        public List<PartitionDefinition> partitions { get; set; }
    }

    class PartitionDefinition
    {
        public string partition_name { get; set; }
        public PartitionSourceObjectDefinition source { get; set; }
        public List<string> partition_filters { get; set; }
    }

    class PartitionSourceObjectDefinition
    {
        public string original_object_name { get; set; }
        public string new_object_name { get; set; }
    }

    class TenantDatasetGenerator
    {
        private int _tenantId;
        private Model _model;
        private Database _database;
        private string _datasetSource;

        public TenantDatasetGenerator(int tenantId, Database templateDatabase, string datasetSource)
        {
            _tenantId = tenantId;
            _database = templateDatabase;
            _model = templateDatabase.Model;
            _datasetSource = datasetSource;
            PartitionTables();
        }

        private void PartitionTables()
        {
            foreach (Table tb in _model.Tables)
            {
                string tbName = tb.Name;
                if (tb.Partitions[0].Source.GetType() == typeof(CalculatedPartitionSource) || !tb.Columns.Contains("TerminalId"))
                {
                    //Change only the partitions source
                    if (!String.IsNullOrEmpty(_datasetSource) & !(tb.Partitions[0].Source.GetType() == typeof(CalculatedPartitionSource)))
                    {
                        //For all the partitions in the table replace the source
                        foreach(Partition partition in tb.Partitions)
                        {
                            MPartitionSource mPartition = (MPartitionSource)partition.Source;
                            var mPartitionCodeLines = mPartition.Expression.Split('\n');
                            string sourceLine = mPartitionCodeLines[1];
                            if (sourceLine.ToLower().Contains("databricks.catalogs"))
                            {
                                string newSourceLine = "    " + sourceLine.Split('=')[0].Trim() + " = " + _datasetSource + ",";
                                mPartitionCodeLines[1] = newSourceLine;
                                string newPartitionCode = String.Join(System.Environment.NewLine, mPartitionCodeLines);
                                mPartition.Expression = newPartitionCode;
                            }
                            else
                            {
                                continue;
                            }
                        }
                    }
                    continue;
                }
                string filterTenantExpression = "    #\"Filtered Tenant\" = Table.SelectRows(##previous_step_name##, each ([TerminalId] = ##var_terminal_id##))";
                foreach (Partition partition in tb.Partitions)
                {
                    if (partition.Source.GetType() == typeof(MPartitionSource))
                    {
                        MPartitionSource mPartition = (MPartitionSource)(partition.Source);
                        string partitionExpression = mPartition.Expression;
                        string existingMPartitionCode = partitionExpression.Substring(0, partitionExpression.IndexOf("in\n") - 1) + ",\n";
                        var existingMCodeLines = existingMPartitionCode.Split('\n');
                        string lastMCodeLine = existingMCodeLines[existingMCodeLines.Length - 2];
                        string lastStepName = lastMCodeLine.Split('=')[0].Trim();
                        if (existingMPartitionCode.Contains("[TerminalId]"))
                        {
                            throw new Exception("Operations on TerminalId column already detected on the table [" + tb.Name + "] partition");
                        }
                        string filteredTenantPartitionCode = existingMPartitionCode + filterTenantExpression.Replace("##previous_step_name##", lastStepName).Replace("##var_terminal_id##", _tenantId.ToString()) + "\n";
                        //Use filtered tenant partition code to replace the Source step if set
                        if (!String.IsNullOrEmpty(_datasetSource))
                        {
                            var filteredMPartitionCodeLines = filteredTenantPartitionCode.Split('\n');
                            string sourceLine = filteredMPartitionCodeLines[1];
                            string newSourceLine = "    " + sourceLine.Split('=')[0].Trim() + " = " + _datasetSource + ",";
                            filteredMPartitionCodeLines[1] = newSourceLine;
                            filteredTenantPartitionCode = String.Join(System.Environment.NewLine, filteredMPartitionCodeLines);
                        }
                        string newPartitionExpression = filteredTenantPartitionCode + "in\n    #\"Filtered Tenant\"";
                        mPartition.Expression = newPartitionExpression;
                    }
                }
            }
        }

        public static List<Database> GeneratePartitionedDatabases(Database templateDatabase, string databaseName, PartitioningModel partitionModel, string tenantCode)
        {
            List<Database> resultDatasets = new List<Database>();
            //Iterate for each dataset to be created from the definition
            foreach(DatasetModel datasetModel in partitionModel.datasets)
            {
                Database currentDataset = templateDatabase.Clone();
                string currentDatasetName = datasetModel.dataset_name.Replace("##dataset_name##", databaseName).Replace("##tenant_code##", tenantCode);
                //Iterate for each table to be partitioned
                foreach(DatasetDefinition definition in datasetModel.definition)
                {
                    string targetTable = definition.table_name;
                    Table currentTable = currentDataset.Model.Tables.Find(targetTable);

                    if (currentTable.Partitions.Count > 1)
                    {
                        throw new Exception("Table has more than one partition in the model");
                    }

                    MPartitionSource templatePartition = (MPartitionSource)currentTable.Partitions[0].Source;
                    string templatePartitionExpression = templatePartition.Expression;
                    currentTable.Partitions.Remove(currentTable.Partitions[0]);

                    //Iterate for each partition to exist on the table
                    foreach(PartitionDefinition partitionDefinition in definition.partitions)
                    {
                        string currentPartitionExpression = templatePartitionExpression;
                        string partitionName = partitionDefinition.partition_name;
                        
                        //Check if there is a replace on the source object
                        if (!String.IsNullOrEmpty(partitionDefinition.source.original_object_name))
                        {
                            currentPartitionExpression = currentPartitionExpression.Replace(partitionDefinition.source.original_object_name, partitionDefinition.source.new_object_name);
                        }

                        MPartitionSource currentPartitionSource = new MPartitionSource();
                        currentPartitionSource.Expression = currentPartitionExpression;

                        //For each partition filter add the filter steps to the expression
                        if(partitionDefinition.partition_filters.Count > 0)
                        {
                            string filterExpression = String.Join(" and ", partitionDefinition.partition_filters.Select(x => $"({x})"));
                            string lastExpressionStep = GetLastExpressionStepName(currentPartitionSource.Expression);
                            string filterStepTemplate = $"#\"Filtered Rows Partitioned\" = Table.SelectRows({lastExpressionStep}, each {filterExpression})";
                            string newPartitionExpression = AddPartitionStep(filterStepTemplate, currentPartitionSource);
                            currentPartitionSource.Expression = newPartitionExpression;
                        }

                        Partition newPartition = new Partition
                        {
                            Name = partitionName,
                            Source = currentPartitionSource
                        };

                        currentTable.Partitions.Add(newPartition);
                    }
                }
                currentDataset.Model.Name = currentDatasetName;
                if (!Directory.Exists("./output/partitioned/"))
                {
                    Directory.CreateDirectory("./output/partitioned");
                }
                SaveModelBimFromDatabase($"./output/partitioned/{currentDatasetName}.bim", currentDataset);
                resultDatasets.Add(currentDataset);
            }

            return resultDatasets;
        }

        public static string DeployDatabase(string connectionString, Database database, string databaseName, string tenantCode)
        {
            //Add code to partition the model => this can generate more than 1 dataset to be deployed
            PartitioningModel partitionModel = JsonConvert.DeserializeObject<PartitioningModel>(File.ReadAllText("./Configs/model_partitions.json"));
            List<Database> resultDatasets =  GeneratePartitionedDatabases(database, databaseName, partitionModel, tenantCode);

            string datasetId = String.Empty;
            foreach (Database dataset in resultDatasets)
            {
                Server server = ConnectToServer(connectionString);
                if (server.Databases.FindByName(dataset.Model.Name) != null)
                {
                    Console.WriteLine("Updating database " + dataset.Model.Name);
                    UpdateDatabaseInServer(server, dataset, dataset.Model.Name);
                }
                else
                {
                    Console.WriteLine("Creating database " + dataset.Model.Name);
                    CreateDatabaseInServer(server, dataset, dataset.Model.Name);
                }

                if(dataset.Model.Name == databaseName)
                {
                    datasetId = server.Databases.GetByName(dataset.Model.Name).ID;
                }
            }

            return datasetId;

            //if (server.Databases.FindByName(databaseName) != null)
            //{
            //    Console.WriteLine("Updating database " + databaseName);
            //    UpdateDatabaseInServer(server, database, databaseName);
            //}
            //else
            //{
            //    Console.WriteLine("Creating database " + databaseName);
            //    CreateDatabaseInServer(server, database, databaseName);
            //}

            //return server.Databases.GetByName(databaseName).ID;
        }

        private static Server ConnectToServer(string connectionString)
        {
            Server svr = new Server();
            Console.WriteLine("Connecting to Server");
            svr.Connect(connectionString);
            Console.WriteLine("Server Connected");
            return svr;
        }

        private static void CreateDatabaseInServer(Server server, Database database, string databaseName)
        {
            string newDatabaseName = server.Databases.GetNewName(databaseName);
            database.Name = newDatabaseName;
            //database.CompatibilityLevel = server.DefaultCompatibilityLevel;
            server.Databases.Add(database);
            database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        }

        private static void UpdateDatabaseInServer(Server server, Database database, string databaseName)
        {
            Database svrDatabase = server.Databases.GetByName(databaseName);
            ModelRoleCollection existingRoles = svrDatabase.Model.Roles;
            foreach(ModelRole role in existingRoles)
            {
                foreach(ModelRoleMember member in role.Members)
                {
                    ModelRoleMember newMember = member.Clone();
                    database.Model.Roles[role.Name].Members.Add(newMember);
                }
            }
            database.Model.CopyTo(svrDatabase.Model);
            svrDatabase.Model.SaveChanges();
            CalculateModel(server, databaseName);
        }

        private static void CalculateModel(Server server, string databaseName)
        {
            server.Databases.FindByName(databaseName).Model.RequestRefresh(RefreshType.Calculate);
            server.Databases.FindByName(databaseName).Model.SaveChanges();
        }

        private static string GetLastExpressionStepName(string partitionExpression)
        {
            var existingMCodeLines = partitionExpression.Split('\n');
            string lastMCodeLine = existingMCodeLines[existingMCodeLines.Length - 3];
            string lastStepName = lastMCodeLine.Split('=')[0].Trim();
            return lastStepName;
        }

        private static string AddPartitionStep(string stepExpression, MPartitionSource partition)
        {
            string partitionExpression = partition.Expression;
            string existingMPartitionCode = partitionExpression.Substring(0, partitionExpression.IndexOf("in\n") - 1) + ",\n";
            string filteredTenantPartitionCode = existingMPartitionCode + "    " + stepExpression + "\n";
            string lastStepName = stepExpression.Split('=')[0].Trim();
            string newPartitionExpression = filteredTenantPartitionCode + "in\n    " + lastStepName;
            //partition.Expression = newPartitionExpression;
            return newPartitionExpression;
        }

        public Database SaveModelBim(string savePath)
        {
            Database outDatabase = _database;
            string outModelBim = Microsoft.AnalysisServices.Tabular.JsonSerializer.SerializeDatabase(outDatabase);
            File.WriteAllText(savePath, outModelBim);
            return outDatabase;
        }

        public static void SaveModelBimFromDatabase(string savePath, Database database)
        {
            Database outDatabase = database;
            string outModelBim = Microsoft.AnalysisServices.Tabular.JsonSerializer.SerializeDatabase(outDatabase);
            File.WriteAllText(savePath, outModelBim);
        }
    }

}
