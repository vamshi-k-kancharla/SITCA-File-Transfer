using Microsoft.AspNetCore.Mvc;
using MongoDB.Driver;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;


using SITCAFileTransferService.Common;

namespace SITCAFileTransferService.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class FileTransferController : ControllerBase
    {
        private readonly ILogger<FileTransferController>? logger;
        public static IMongoDatabase currentDataBase = null;

        /// <summary>
        /// File transfer controller class & constructor.
        /// </summary>
        /// 
        /// <param name="inputLogger"> injected input logger object.</param>
        /// 
        public FileTransferController(ILogger<FileTransferController> inputLogger)
        {
            var dbConnection = new MongoClient("mongodb://localhost:27017");
            currentDataBase = dbConnection.GetDatabase("FileTransferDB");

            logger = inputLogger;
        }

        /// <summary>
        /// Loads the input file into database of File Parts data.
        /// </summary>
        /// 
        /// <param name="fileName"> Name of the input file to be loaded.</param>
        /// 
        /// <returns> Returns success / failure response of file loading operation.</returns>

        [HttpGet]
        [Route("")]
        [Route("LoadFile/{fileName?}")]
        public IResult LoadFile(string? fileName)
        {

            string retValueString = "";
            FileStream currentFS = null;

            try
            {
                List<Thread> fileReadThreads = new List<Thread>();

                IMongoCollection<FilePartsData> currentCollection = DataHelperUtils.CreateDBCollection(currentDataBase, 
                    fileName);

                string fileNameFQDN = FileTransferServerConfig.inputFilePath + fileName;

                currentFS = System.IO.File.Open(fileNameFQDN, FileMode.Open, FileAccess.ReadWrite);

                retValueString += "File is opened for Read/Write operations , ";

                // ToDo : Retrieve file size automatically.

                long numOfSubParts = ( FileTransferServerConfig.fileSize % FileTransferServerConfig.chunkSize == 0 ) ?
                    ( FileTransferServerConfig.fileSize / FileTransferServerConfig.chunkSize ) :
                    ((FileTransferServerConfig.fileSize / FileTransferServerConfig.chunkSize) + 1 );

                long totalNoOfCurrentThreadParts = 0;

                //long numberOfThreads = numOfThreads;

                long numberOfPartsInSubPart = ( numOfSubParts % FileTransferServerConfig.numberOfThreads == 0 ) ?
                    ( numOfSubParts / FileTransferServerConfig.numberOfThreads ) :
                    ( numOfSubParts / FileTransferServerConfig.numberOfThreads + 1);

                for ( long currentThreadPart = 0; currentThreadPart < numOfSubParts; 
                    currentThreadPart += numberOfPartsInSubPart )
                {
                    long numberOfPartsInLastChunk = 0;

                    if ( currentThreadPart + numberOfPartsInSubPart > numOfSubParts )
                    {
                        numberOfPartsInLastChunk = numOfSubParts - currentThreadPart;

                    }

                    LoadThreadObject fileReadParamObj = new LoadThreadObject();

                    fileReadParamObj.currentDB = currentDataBase;
                    fileReadParamObj.fileName = fileName;

                    fileReadParamObj.currentOffset = currentThreadPart * FileTransferServerConfig.chunkSize;
                    fileReadParamObj.startPart = currentThreadPart;

                    fileReadParamObj.numOfSubParts = (numberOfPartsInLastChunk != 0) ? numberOfPartsInLastChunk :
                        numberOfPartsInSubPart;

                    fileReadParamObj.currentCollection = currentCollection;
                    fileReadParamObj.currentFS = currentFS;

                    totalNoOfCurrentThreadParts = currentThreadPart + fileReadParamObj.numOfSubParts;


                    if (FileTransferServerConfig.bFirstLevelDebug == true)
                    {

                        Console.WriteLine("Thread is being fired with the following context => fileName = " + fileName +
                        " ,currentOffset = " + fileReadParamObj.currentOffset +
                        " ,currentIterationCount = " + fileReadParamObj.currentIterationCount);
                    }

                    Thread currentIterationThread = new Thread(SITCAFileLoadAndReadThread.FileLoadAndReadThread);
                    currentIterationThread.Start(fileReadParamObj);

                    fileReadThreads.Add(currentIterationThread);
                }

                if (FileTransferServerConfig.bFirstLevelDebug == true)
                {

                    Console.WriteLine("=====================================================");
                }

                // Add Total number of Parts Data

                byte[] noOfPartsByteArray = DataHelperUtils.ConvertIntToByteArray((int)totalNoOfCurrentThreadParts);

                FilePartsData numberOfFilePartsAddedData = DataHelperUtils.AddDataToCollection(
                    (int)totalNoOfCurrentThreadParts + 1,
                    "NumberOfFileParts", noOfPartsByteArray);

                currentCollection.InsertOne(numberOfFilePartsAddedData);


                if (FileTransferServerConfig.bFirstLevelDebug == true)
                {

                    Console.WriteLine(" , End of read stream , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                    Console.WriteLine("=========================================================================");
                }

                while (true)
                {
                    if (SITCAFileLoadAndReadThread.AreAllThreadsStopped(fileReadThreads))
                    {
                        break;
                    }

                    Console.WriteLine(" Some of the file read threads are still running...Sleep for some time");

                    Thread.Sleep(2000);
                }

                currentFS.Close();

                retValueString += "file read is successful";

            }

            catch (Exception e)
            {

                if(logger != null)
                {
                    logger.LogError("Exception occured while Loading the file into FilePartsData : Exception = " + e.Message);
                }

                if (currentFS != null)
                {
                    currentFS.Close();
                }

                return Results.BadRequest("Exception occured while loading the input file  : exception = " + e.Message);
            }

            return Results.Ok(retValueString);
        }

        /// <summary>
        /// Gets the file part data retrieved from Mongo DB.
        /// </summary>
        /// 
        /// <param name="fileName"> Name of the input file to retrieve the data from.</param>
        /// <param name="filePartName"> Name of the file part to be retrieved.</param>
        /// 
        /// <returns> File part data in string format.</returns>

        [HttpGet]
        [Route("GetFilePartData/{fileName?}/{filePartName?}")]
        public IResult GetFilePartData(string fileName, string filePartName)
        {

            byte[] retValueFilePartsData = new byte[FileTransferServerConfig.chunkSize];
            string retValueString = "";

            try
            {

                // Retrieve the collection based on fileName
                
                IMongoCollection<FilePartsData> currentCollection = currentDataBase.GetCollection<FilePartsData>(fileName);


                if (FileTransferServerConfig.bFirstLevelDebug == true)
                {

                    Console.WriteLine("=========================================================================");
                    Console.WriteLine(" , Before query execution  , current time = " + DateTime.Now.Hour + ":" +
                        DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);
                }

                IFindFluent<FilePartsData, FilePartsData> queriedFilePartData  = 
                    currentCollection.Find(x => x.filePartName == filePartName);

                if ( queriedFilePartData == null || queriedFilePartData.FirstOrDefault() == null
                    || queriedFilePartData.FirstOrDefault().filePartData == null )
                {
                    Console.Write("Null data found for input query");
                    throw new ArgumentNullException("File data for the input query doesn't exist");
                }

                retValueFilePartsData = queriedFilePartData.FirstOrDefault().filePartData;

                if (FileTransferServerConfig.bFirstLevelDebug == true)
                {

                    Console.WriteLine(" , After query execution , before processing = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                    Console.WriteLine("GetFilePartData : Read bytes written into a buffer for processing");
                }

                if (filePartName == "NumberOfFileParts")
                {

                    for (int i = 0; i < retValueFilePartsData.Length; i++)
                    {
                        retValueString += (char)(retValueFilePartsData[i]);
                    }

                    return Results.Ok(retValueString);
                }
                else
                {
                    return Results.Ok(retValueFilePartsData);
                }

            }

            catch (Exception e)
            {
                logger.LogInformation("Exception occured while querying the file parts Data : Exception = " + e.Message);
                return Results.BadRequest("Exception occured while querying the file parts Data  : exception = " + e.Message);
            }

            return Results.Ok(retValueString);
        }

    }

}