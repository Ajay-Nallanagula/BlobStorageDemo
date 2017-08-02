using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System.Configuration;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Auth;
using System.IO;
using BlobStorageDemo.Models.TableEntites;
using Microsoft.WindowsAzure.Storage.Queue;


//https://docs.microsoft.com/en-us/azure/storage/storage-dotnet-how-to-use-blobs
namespace AzureStorageDemo
{
    class Program
    {
        static void Main()
        {

            Console.WriteLine("Enter your choice of Storage type, example: 2");
            Console.WriteLine("1.Blob");
            Console.WriteLine("2.Table");
            Console.WriteLine("3.Queue");
            Console.WriteLine("4.File");

            var choice = Convert.ToInt32(Console.ReadLine());

            switch (choice)
            {
                case 1:
                    {
                        Console.WriteLine("Your Choice is Blob Storage");
                        var blobClientAppConfig = FetchConnectionStringAppConfig();

                        Console.WriteLine("Please enter the container name, If mentioned container doesn't exist new container will be created");
                        string containerName = Console.ReadLine(); 

                        var cloudBlobContainer = GetContainerRefrence(containerName, blobClientAppConfig);

                        Console.WriteLine($"Do you wish to {Environment.NewLine} 1.Create BlockBlob {Environment.NewLine} 2. AppendBlob ?");
                        var response = Convert.ToInt32(Console.ReadLine());
                        if (response == 1)
                        {
                            CreateBlockBlob(cloudBlobContainer);
                        }
                        else if (response == 2)
                        {
                            CreateAppendBlob(cloudBlobContainer);
                        }
                        else
                        {
                            Console.WriteLine("Please make a valid choice!");
                        }

                        break;
                    }

                case 2:
                    {
                        Console.WriteLine("Your Choice is Table Storage");
                        var cloudTableClient = CreateTableClient();
                        CloudTable cloudTable = GetTableReference(cloudTableClient);
                        Console.WriteLine("Do you want insert one row at a time or batch of rows?"); Console.WriteLine("1.One row"); Console.WriteLine("2.Batch row"); Console.WriteLine("3.Retrieve All the rows");
                        var choi = Convert.ToInt32(Console.ReadLine());
                        switch (choi)
                        {
                            case 1:
                                TableResult tableResult = InsertDataToTable(cloudTable);
                                break;
                            case 2:
                                var tableBatchResult = BatchInsertTableData(cloudTable);
                                break;
                            case 3:
                                DisplayAllRows(cloudTable);
                                break;
                            default:
                                Console.WriteLine("Choice you entered is not valid!!, Please Try Again");
                                break;
                        }
                        break;
                    }

                case 3:
                    {
                        var queueClient = CreateQueueClient();
                        CloudQueue cloudQueue = CreateQueueStorage(queueClient);

                        Console.WriteLine($"Choose one of the activity below");
                        Console.WriteLine("1. Enqueue New Message");
                        Console.WriteLine("2. Update existing message");
                        Console.WriteLine("3. Dequeue the messages one message");
                        Console.WriteLine("4. Dequeue all messages");
                        Console.WriteLine("5.Delete The queue");
                        var resp = Convert.ToInt32(Console.ReadLine());

                        switch (resp)
                        {
                            case 1:
                                {
                                    string acknowledge = InsertQueueMessage(cloudQueue) ? "New Message has been inserted in the Queue" : "Oops something went wrong couldn't insert the message to Queue";
                                    Console.WriteLine(acknowledge);
                                    break;
                                }
                            case 2:
                                {
                                    UpdateExistingMessage(cloudQueue);
                                    break;
                                }
                            case 3:
                                {
                                    var str = DequeueOneMessages(cloudQueue);
                                    Console.WriteLine($"Message That is DeQueued is : '{str}'");
                                    break;
                                }
                            case 4:
                                {
                                    DeQueueAllMessages(cloudQueue);
                                    break;
                                }
                            case 5:
                                {
                                    DeleteQueue(cloudQueue);
                                    break;
                                }
                            default:
                                break;
                        }

                        break;
                    }
                default:
                    {
                        Console.WriteLine("Please make a valid choice!");
                        break;
                    }
            }

            Console.ReadKey();

        }




        #region Queue Storage

        private static void DeleteQueue(CloudQueue cloudQueue)
        {
            Console.WriteLine($"Are You sure you want to delete the queue : Y/N");
            var resp = Console.ReadLine();
            if (resp.ToUpper().Equals("Y"))
            {
                cloudQueue.Delete();
            }
            Console.WriteLine($"Queue Deleted successfully");
        }

        private static void DeQueueAllMessages(CloudQueue cloudQueue)
        {
            Console.WriteLine($"Are You sure you want to delete all the messages : Y/N");
            var resp = Console.ReadLine();
            if (resp.ToUpper().Equals("Y"))
            {

                int nofMessages = 2; //  This number should be less than 33, because 32 is the limit 
                var messagess = cloudQueue.GetMessages(nofMessages, TimeSpan.FromSeconds(2));//For 2 seconds the messages will not be visible to anyone
                for (int i = 0; i < messagess.Count(); i++)
                {
                    Console.WriteLine("Message thats is been deleted: " + messagess.ToList()[i].AsString);
                    cloudQueue.DeleteMessage(messagess.ToList()[i]);
                }
            }
        }

        private static string DequeueOneMessages(CloudQueue cloudQueue)
        {
            //De-queue happens in two steps 

            //Step 1: GetMessage(), this methods will make the message invisible for 30 seconds, however this can be increased or decreased TimeSpan param
            CloudQueueMessage msg = cloudQueue.GetMessage();
            //Step 2 : This message can be deleted .
            cloudQueue.DeleteMessage(msg);
            return msg.AsString;
        }

        private static void UpdateExistingMessage(CloudQueue cloudQueue)
        {
            var getMsg = cloudQueue.GetMessage();
            getMsg.SetMessageContent("Updated Contents");
            //Make it invisible for another 60 seconds.
            cloudQueue.UpdateMessage(getMsg, TimeSpan.FromSeconds(60.0), MessageUpdateFields.Content | MessageUpdateFields.Visibility);
        }

        /// <summary>
        /// Insert the message in the Queue by using methods on Queuestorage
        /// </summary>
        /// <param name="cloudQueue">Queuestorage reference</param>
        private static bool InsertQueueMessage(CloudQueue cloudQueue)
        {
            //For ApproximateMessageCount to work and return number of Messages FetchAttributes() needs to be called which would update the latest stats 
            cloudQueue.FetchAttributes();
            var msgCountBeforeInserting = cloudQueue.ApproximateMessageCount ?? 0;
            Console.WriteLine($"Please enter the message that you want to enter ");
            var contentMessage = Console.ReadLine();
            CloudQueueMessage cloudQueueMessage = new CloudQueueMessage(contentMessage);
            cloudQueue.AddMessage(cloudQueueMessage);
            //For ApproximateMessageCount to work and return number of Messages FetchAttributes() needs to be called which would update the latest stats 
            cloudQueue.FetchAttributes();
            return cloudQueue.ApproximateMessageCount > msgCountBeforeInserting;
        }

        /// <summary>
        /// Creation of Queue storage account , to store the queue messages 
        /// </summary>
        /// <param name="queueClient">QueueClient  gives us the reference to Queuestorage if already exists, else will create new queue storage</param>
        /// <returns></returns>
        private static CloudQueue CreateQueueStorage(CloudQueueClient queueClient)
        {
            Console.WriteLine($"Please enter the Queue Storage name, If Queue Storage doen't exist it will be created");
            var queueStorageName = Console.ReadLine();
            CloudQueue cloudQueue = queueClient.GetQueueReference(queueStorageName);
            var isNewQueueStorageCreated = cloudQueue.CreateIfNotExists();
            if (isNewQueueStorageCreated)
            {
                Console.WriteLine($"New Queue Storage is created {queueStorageName}");
            }

            return cloudQueue;
        }

        /// <summary>
        /// Creates CloudQueueClient, after fetching reference of storage account
        /// </summary>
        /// <returns></returns>
        private static CloudQueueClient CreateQueueClient()
        {
            var cloudStorageAccount = ParseConnectionStr();
            CloudQueueClient cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            return cloudQueueClient;
        }

        #endregion Queue Storage

        #region TableStorage

        private static void DisplayAllRows(CloudTable cloudTable)
        {
            var tableQuery = new TableQuery<CustomerEntity>();

            var empList = cloudTable.ExecuteQuery(tableQuery).ToList();

            foreach (var emp in empList)
            {
                Console.Write($"{emp.RowKey}   ");
                Console.Write($"{emp.PartitionKey}   ");
                Console.Write($"{emp.Email}  ");
                Console.Write($"{emp.Phone}  ");
                Console.WriteLine();
            }
        }

        private static CloudTableClient CreateTableClient()
        {
            var connection = ParseConnectionStr();
            CloudTableClient cloudTableClient = connection.CreateCloudTableClient();
            return cloudTableClient;
        }

        private static TableResult InsertDataToTable(CloudTable cloudTable)
        {
            Console.WriteLine("Inserting Customer data to table");
            var cust = new CustomerEntity("Nallanagula", "Ajay") { Email = "hi@hts.com", Phone = "040123456" };
            TableOperation tblOperation = TableOperation.Insert(cust);
            return cloudTable.Execute(tblOperation);

        }

        private static IList<TableResult> BatchInsertTableData(CloudTable cloudTable)
        {
            TableBatchOperation batchOperation = new TableBatchOperation();
            for (int i = 0; i < 10; i++)
            {
                string partionKey = "Nallanagula";
                string rowKey = "Ajay" + i;
                batchOperation.Insert(new CustomerEntity(partionKey, rowKey)
                {
                    Email = "haiku@haiku.com" + i,
                    Phone = "12345" + i
                });
            }
            return cloudTable.ExecuteBatch(batchOperation);
        }

        private static CloudTable GetTableReference(CloudTableClient cloudTableClient)
        {
            Console.WriteLine("Enter The table name, If the Table doesn't exist new table will be created for you");
            var tableStoragename = Console.ReadLine();

            CloudTable cloudTable = cloudTableClient.GetTableReference(tableStoragename);//"ajTableStorage"
            cloudTable.CreateIfNotExists();

            return cloudTable;
        }

        #endregion TableStorage

        #region AppendBlob
        private static void CreateAppendBlob(CloudBlobContainer cloudBlobContainer)
        {
            Console.WriteLine("Please Enter the Append Blob Name");
            var appendBlobName = Console.ReadLine();
            CloudAppendBlob cloudAppendBlob = cloudBlobContainer.GetAppendBlobReference(appendBlobName);
            var isNewAppendBlob = !cloudAppendBlob.Exists();
            cloudAppendBlob.CreateOrReplace(); //New Blob Creation or Replace blob
            if (isNewAppendBlob)
            {
                Console.WriteLine($"{appendBlobName} doesn't exist, hence new Blob is created with the name {appendBlobName}");
            }

            for (int i = 0; i < 10; i++)
            {
                cloudAppendBlob.AppendText($"{DateTime.Now}  {Guid.NewGuid()} {Environment.NewLine}");
            }

            Console.WriteLine($"Do You want To Download Append Blob {appendBlobName} Y/N ?");
            var choice = Console.ReadLine();
            if (choice.ToUpper().Equals("Y"))
            {
                using (var stream = new FileStream($@"D:\TEXTBOOKS_REFERENCEES\AzureCodeDownloads\AppendBlobEx{Guid.NewGuid()}.txt", FileMode.Create))
                {
                    cloudAppendBlob.DownloadToStream(stream);
                }
            }
        }

        #endregion AppendBlob

        #region BlockBlob

        /// <summary>
        /// Creates a new Block Blob
        /// </summary>
        /// <param name="cloudBlobContainer">Blb should reside inside a container, hence blob container passed as parameter</param>
        private static void CreateBlockBlob(CloudBlobContainer cloudBlobContainer)
        {
            try
            {

                Console.WriteLine("Do you want to upload Y/N ?");
                var isUpload = Console.ReadLine();

                Console.WriteLine("Enter the name of Blob , If blob doesn't exist new blob will be created for you.");
                var cloudBlobName = Console.ReadLine();
                string pathToUpLoad = string.Empty;

                //Upload Blob To a Container :
                if (isUpload.ToUpper().Equals("Y"))
                {
                    Console.WriteLine(@"Please Enter the destination path of file to be uploaded :path\Name.txt format");
                    pathToUpLoad = Console.ReadLine();
                    // pathToUpLoad = @"D:\TEXTBOOKS_REFERENCEES\AzureCodeDownloads\SomeSampleImage.jpg";
                    UploadBlobToContainer(cloudBlobName, cloudBlobContainer, pathToUpLoad);
                }

                Console.WriteLine("Do you want to download Y/N ?");
                var isDownload = Console.ReadLine();

                if (isDownload.ToUpper().Equals("Y"))
                {
                    Console.WriteLine(@"Please Enter the destination path where to download :path\Name.jpg format");
                    pathToUpLoad = Console.ReadLine();
                    //pathToUpLoad = @"D:\TEXTBOOKS_REFERENCEES\AzureCodeDownloads\SomeSampleImageDL3.jpg";
                    DownloadBlobF(cloudBlobName, cloudBlobContainer, pathToUpLoad);
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                Console.WriteLine($"Error Message: {msg}");
            }
        }

        private static void UploadBlobToContainer(string blobName, CloudBlobContainer cloudBlobContainer, string path)
        {
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            using (var fileStream = File.OpenRead(path))
            {
                cloudBlockBlob.UploadFromStream(fileStream);
            }
        }

        private static void DownloadBlobF(string blobName, CloudBlobContainer cloudBlobContainer, string path)
        {
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);

            using (var fileStream = File.Create(path))
            {
                cloudBlockBlob.DownloadToStream(fileStream);
            }
        }

        private static void FetchSetBlobProperties(CloudBlockBlob cloudBlockBlob)
        {
            cloudBlockBlob.FetchAttributes();
            var contentType = cloudBlockBlob.Properties.ContentType;
            Console.WriteLine($"Exisiting ContentType {contentType}");
            Console.WriteLine("Do You wish to change the content type Y/N");
            var response = Console.ReadLine();
            if (response.ToUpper().Equals("Y"))
            {
                cloudBlockBlob.Properties.ContentType = @"img\jpg";
                cloudBlockBlob.SetProperties();
                Console.WriteLine($"Blob Content Type is changed to {cloudBlockBlob.Properties.ContentType}");
            }
        }

        private static CloudBlobClient FetchConnectionStringAppConfig()
        {
            CloudStorageAccount cloudStorageAccountAppConfig = ParseConnectionStr();
            var blobClientAppConfig = cloudStorageAccountAppConfig.CreateCloudBlobClient();
            return blobClientAppConfig;
        }

        #endregion BlockBlob

        #region Container
        private static CloudBlobContainer GetContainerRefrence(string containerName, CloudBlobClient blobClientAppConfig)
        {
            var cloudBlobContainer = blobClientAppConfig.GetContainerReference(containerName);
            var isContainerCreated = cloudBlobContainer.CreateIfNotExists();
            if (isContainerCreated)
            {
                Console.WriteLine($"New Container with name: '{containerName}' has been created");
            }

            //By default, the new container is private, meaning that you must specify your storage access key to download blobs from this container
            //To Make it accesssible to every one we are making it as public  
            var blobContainerPermissions = new BlobContainerPermissions();
            blobContainerPermissions.PublicAccess = BlobContainerPublicAccessType.Blob;
            cloudBlobContainer.SetPermissions(blobContainerPermissions);

            return cloudBlobContainer;
        }

        #endregion Container

        #region ConnectionString

        /// <summary>
        /// Way 1: Accessing Connection string from App.Config/Web.Config
        /// KeyPoints, Make sure there are no whitespace in the connection string stored in App.config  , That will cause an exception saying No Key Valid Combination found
        /// </summary>
        /// <returns></returns>
        private static CloudStorageAccount ParseConnectionStr()
        {
            var connStr = CloudConfigurationManager.GetSetting("StorageConnection");
            CloudStorageAccount cloudStorageAccountAppConfig = CloudStorageAccount.Parse(connStr);
            return cloudStorageAccountAppConfig;
        }

        /// <summary>
        /// Creating and accessing cloud storage via StorageCredentials. 
        /// </summary>
        /// <returns></returns>
        private static CloudBlobClient FetchConnectionStorageCredentialManager()
        {
            string accountName = CloudConfigurationManager.GetSetting("AccountName");
            string accessKey = CloudConfigurationManager.GetSetting("AccessKey");
            StorageCredentials storageCredentials = new StorageCredentials(accountName, accessKey);
            CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(storageCredentials, false);
            return cloudStorageAccount.CreateCloudBlobClient();
        }

        /// <summary>
        /// This is used in Development environment only, This is Ffor emulator for debugging purpose in development environment 
        /// The data is created in Server :(localdb)\MSSQLLocalDb  DB : AzureStorageEmulatorDb51 by default
        /// </summary>
        /// <returns></returns>
        private static CloudBlobClient FetchConnectionStringEmulator()
        {
            //Connection string is shortcut way to connect to AzureEmulator
            var connStr = CloudConfigurationManager.GetSetting("EmulatorConnectionString");
            CloudStorageAccount cloudStorageAccountAppConfig = CloudStorageAccount.Parse(connStr);
            var blobClientAppConfig = cloudStorageAccountAppConfig.CreateCloudBlobClient();
            return blobClientAppConfig;
        }

        /// <summary>
        /// This is used in Development environment only, This is Ffor emulator for debugging purpose in development environment 
        /// The data is created in Server :(localdb)\MSSQLLocalDb  DB : AzureStorageEmulatorDb51 by default
        /// </summary>
        /// <returns></returns>
        private static CloudBlobClient FetchConnectionEmulatorStorageCredentialManager()
        {
            string accountName = CloudConfigurationManager.GetSetting("AccountNameEmulator");
            string accessKey = CloudConfigurationManager.GetSetting("AccessKeyEmulator");
            StorageCredentials storageCredentials = new StorageCredentials(accountName, accessKey);
            CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(storageCredentials, false);
            return cloudStorageAccount.CreateCloudBlobClient();
        }

        #endregion ConnectionString



    }
}
