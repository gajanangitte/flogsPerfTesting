using Microsoft.Extensions.Logging;

using Azure.Core;
using Azure.Core.Pipeline;

using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;

using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;

using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Linq;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

using System.Security;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace LogGenerator
{
    public class Function1
    {
        [FunctionName("Function1")]
        public async Task Run([TimerTrigger("0 0,15,30,45 * * * *")]TimerInfo myTimer, ILogger log)
        {

            string containerName = "insights-logs-flowlogflowevent";
            string downStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=aahilstorage;AccountKey=nA4VhfQzZBo3tRtoiyeW8NnNtERwZPtw7u/uBb6KFN5Y3o+YA4SjznZddkZCxCddXBxbTfp8IA6Z+AStTgk7rw==;EndpointSuffix=core.windows.net";
            List<string> logStorageAccountStrings = getLogStorageConnectionStrings(log);
        

            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            log.LogInformation("Downloading the flowlog i.e. to be ingested, size: 2.05MB Blob");
            BlobClientOptions blobClientOptions= new BlobClientOptions();
            blobClientOptions.Retry.MaxRetries = 2;
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString: downStorageConnectionString, options: blobClientOptions);
            BlobContainerClient readerClient = blobServiceClient.GetBlobContainerClient("insights-logs-networksecuritygroupflowevent");
            await readerClient.CreateIfNotExistsAsync();
            BlobClient blobreaderClient = readerClient.GetBlobClient("resourceId=/SUBSCRIPTIONS/VNETlog.json");
            string downloadedData = await DownloadToText(blobreaderClient);


            log.LogInformation("Initiating and connecting to a Blob Service Clients to ingest data ... ");
            List<BlobContainerClient> blobContainerClients = new List<BlobContainerClient>();
     
            foreach(string upStorageConnectionString in logStorageAccountStrings)
            {
                blobServiceClient = new BlobServiceClient(connectionString: upStorageConnectionString, options: blobClientOptions);
                
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                await containerClient.CreateIfNotExistsAsync();
                blobContainerClients.Add(containerClient);
            }

            for( int subscriptionNumber = 1000; subscriptionNumber < 2000; subscriptionNumber++)
            {
                foreach(BlobContainerClient containerClient in blobContainerClients)
                {
                    string ToUploadPath = getUploadPath(log, subscriptionNumber);
                    try
                    {
                        log.LogInformation("Getting blobClient ready for :" + ToUploadPath);
                        BlobClient blobClient = containerClient.GetBlobClient(ToUploadPath);

                        log.LogInformation("Uploading Blob ..");
                
                        await blobClient.UploadAsync(BinaryData.FromString(downloadedData), overwrite: true);

                        log.LogInformation("Operation Completed");
                    }
                    catch (Exception ex)
                    {
                        log.LogInformation(ex.ToString());
                    }
                }
            }
            
        }

        private static List<string> getLogStorageConnectionStrings(ILogger log)
        {
            List<string> connectionStrings = new List<string> ();

            for(int accountNumber = 1; accountNumber < 6; accountNumber++)
            {
                string connectionString = "AZURE_STORAGE_LOGS_CONNECTION_STRING_" + accountNumber.ToString();
                
                //log.LogInformation(connectionString);
                connectionString = (string)GetEnvironmentVariable(connectionString);

                if(connectionString.Length > 0)
                   connectionStrings.Add(connectionString);
               
            }

            return connectionStrings;
        }

        public static string GetEnvironmentVariable(string name)
        {
            #nullable enable
            string? environmentVariable = System.Environment.GetEnvironmentVariable(name, System.EnvironmentVariableTarget.Process);
            if (environmentVariable == null)
                return null;

            return environmentVariable;
        }

        public string getUploadPath(ILogger log, int subscriptionNumber)
        {
            string subscriptionID = "AF15E575-F948-49AC-BCE0-252D028E" + subscriptionNumber.ToString();
            string NetworkWatcherRG = "aahilrg";
            string NetworkWatcherName = "NRMS-fuap73iqlrpgcaahilvnet";
            string flowLogName = "vnetFlowLogs2";

            string yearB = Convert.ToString(DateTime.UtcNow.Year);
            string monthB = Convert.ToString(DateTime.UtcNow.Month);
            string dayB = Convert.ToString(DateTime.UtcNow.Day);
            string hourB = Convert.ToString(DateTime.UtcNow.Hour);
            string secondB = Convert.ToString(DateTime.UtcNow.Second);
            string macAddress = "0022482F877B";

            try
            {
                string result = System.String.Format("resourceId=/{0}_{1}/{2}_{3}/y={4}/m={5}/d={6}/h={7}/m=00/macAddress={8}/PT1H.json",
                    subscriptionID, NetworkWatcherRG, NetworkWatcherName, flowLogName, yearB, monthB, dayB, hourB, macAddress);
                return result;
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.ToString());
            }
                return "";
        }

        public static async Task<string> DownloadToText(BlobClient blobClient)
        {

            BlobDownloadResult downloadResult = await blobClient.DownloadContentAsync();
            string downloadedData = downloadResult.Content.ToString();
            return downloadedData;

        }
    }
}
