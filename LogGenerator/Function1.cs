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
        public async Task Run([TimerTrigger("0 5 * * * *")]TimerInfo myTimer, ILogger log)
        {

            string containerName = "insights-logs-flowlogflowevent";
            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=aahilstorage;AccountKey=nA4VhfQzZBo3tRtoiyeW8NnNtERwZPtw7u/uBb6KFN5Y3o+YA4SjznZddkZCxCddXBxbTfp8IA6Z+AStTgk7rw==;EndpointSuffix=core.windows.net";


            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            log.LogInformation("Initiating and connecting to a Blob Service Client ... ");
            BlobClientOptions blobClientOptions;
            BlobServiceClient blobServiceClient;
            blobClientOptions = new BlobClientOptions();
            blobClientOptions.Retry.MaxRetries = 2;
            blobServiceClient = new BlobServiceClient(connectionString: storageConnectionString, options: blobClientOptions);
            log.LogInformation("Action Successful");
            log.LogInformation("Connecting to NSG FLogs ...");
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.CreateIfNotExistsAsync();
            log.LogInformation("Action Successful");

            BlobContainerClient readerClient = blobServiceClient.GetBlobContainerClient("insights-logs-networksecuritygroupflowevent");
            await readerClient.CreateIfNotExistsAsync();
            BlobClient blobreaderClient = readerClient.GetBlobClient("resourceId=/SUBSCRIPTIONS/VNETlog.json");
            string downloadedData = await DownloadToText(blobreaderClient);

            string ToUploadPath = getUploadPath(log);
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

        public string getUploadPath(ILogger log)
        {
            string subscriptionID = "AF15E575-F948-49AC-BCE0-252D028E9379";
            string NetworkWatcherRG = "aahilrg";
            string NetworkWatcherName = "NRMS-fuap73iqlrpgcaahilvnet";
            string flowLogName = "vnetFlowLogs";

            string yearB = Convert.ToString(DateTime.Now.Year);
            string monthB = Convert.ToString(DateTime.Now.Month);
            string dayB = Convert.ToString(DateTime.Now.Day);
            string hourB = Convert.ToString(DateTime.Now.Hour);
            string secondB = Convert.ToString(DateTime.Now.Second);
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
