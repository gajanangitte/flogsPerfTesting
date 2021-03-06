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
        public async Task Run([TimerTrigger("0 0,20,40 * * * *")]TimerInfo myTimer, ILogger log)
        {
            DateTime startTime = DateTime.Now;
            string containerName = "insights-logs-flowlogflowevent";
            string downStorageConnectionString = GetEnvironmentVariable("AZURE_STORAGE_BLOB_CONNECTION_STRING");
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
            Dictionary<BlobContainerClient, string> clientTostring = new Dictionary<BlobContainerClient, string>();
            foreach(string upStorageConnectionString in logStorageAccountStrings)
            {
                blobServiceClient = new BlobServiceClient(connectionString: upStorageConnectionString, options: blobClientOptions);
                
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                await containerClient.CreateIfNotExistsAsync();
                blobContainerClients.Add(containerClient);
                clientTostring.Add(containerClient, upStorageConnectionString.Substring(75,6));
            }


        var options = new ParallelOptions()
        {
            MaxDegreeOfParallelism = Math.Max(4,Environment.ProcessorCount/5)
        };
        
        

        List<int> subscriptionNumbers = new List<int>();
        for(int i=1000; i < 2000; i++)
        {
            subscriptionNumbers.Add(i);
        }


         await Parallel.ForEachAsync( subscriptionNumbers, options, async (subscriptionNumber, token) => 
            {
               
            await Parallel.ForEachAsync( blobContainerClients, options, async (containerClient, token) => {
                    string ToUploadPath = getUploadPath(log, subscriptionNumber, clientTostring[containerClient]);
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
                });
            });

            
            log.LogInformation("Total time taken: "+ (DateTime.Now - startTime));
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

        public string getUploadPath(ILogger log, int subscriptionNumber, string flog)
        {
            string subscriptionID = "AF15E575-F948-49AC-BCE0-252D028E" + subscriptionNumber.ToString();
            string NetworkWatcherRG = "aahilrg";
            string NetworkWatcherName = "NRMS-fuap73iqlrpgcaahilvnet";
            string flowLogName = "vnetFlowLogs2" + flog;

            string yearB = Convert.ToString(DateTime.UtcNow.Year); 
            string monthB = string.Format("{0:00}",DateTime.UtcNow.Month );
            string dayB = string.Format("{0:00}",DateTime.UtcNow.Day );
            string hourB = string.Format("{0:00}",DateTime.UtcNow.Hour );
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
