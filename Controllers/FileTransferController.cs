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

                LoadThreadObject fileReadParamObj = new LoadThreadObject();

                fileReadParamObj.currentDB = currentDataBase;
                fileReadParamObj.fileName = fileName;

                IMongoCollection<FilePartsData> currentCollection = DataHelperUtils.CreateDBCollection(currentDataBase, 
                    fileName);

                fileReadParamObj.currentCollection = currentCollection;

                // ToDo : Retrieve file size automatically.

                int numberOfThreads = ( FileTransferServerConfig.fileSize % FileTransferServerConfig.chunkSize == 0 ) ?
                    ( FileTransferServerConfig.fileSize / FileTransferServerConfig.chunkSize ) :
                    ((FileTransferServerConfig.fileSize / FileTransferServerConfig.chunkSize) + 1 );

                
                for( int threadNum = 0; threadNum < numberOfThreads; threadNum++ )
                {
                    fileReadParamObj.currentOffset = threadNum * FileTransferServerConfig.chunkSize;
                    fileReadParamObj.currentIterationCount = threadNum;

                    Thread currentIterationThread = new Thread(SITCAFileLoadAndReadThread.FileLoadAndReadThread);
                    currentIterationThread.Start();
                }

                Console.WriteLine("=====================================================");

                // Add Total number of Parts Data

                byte[] noOfPartsByteArray = DataHelperUtils.ConvertIntToByteArray(numberOfThreads);

                FilePartsData numberOfFilePartsAddedData = DataHelperUtils.AddDataToCollection(numberOfThreads + 1,
                    "NumberOfFileParts", noOfPartsByteArray);

                currentCollection.InsertOne(numberOfFilePartsAddedData);


                Console.WriteLine(" , End of read stream , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                Console.WriteLine("=========================================================================");

                currentFS.Close();

            }

            catch (Exception e)
            {

                if(logger != null)
                {
                    logger.LogInformation("Exception occured while Loading the file into FilePartsData : Exception = " + e.Message);
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


                Console.WriteLine("=========================================================================");
                Console.WriteLine(" , Before query execution  , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                IFindFluent<FilePartsData, FilePartsData> queriedFilePartData  = 
                    currentCollection.Find(x => x.filePartName == filePartName);

                if ( queriedFilePartData == null || queriedFilePartData.FirstOrDefault() == null
                    || queriedFilePartData.FirstOrDefault().filePartData == null )
                {
                    Console.Write("Null data found for input query");
                    throw new ArgumentNullException("File data for the input query doesn't exist");
                }

                retValueFilePartsData = queriedFilePartData.FirstOrDefault().filePartData;

                Console.WriteLine(" , After query execution , before processing = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                Console.WriteLine("GetFilePartData : Read bytes written into a buffer");

                for ( int i = 0; i < retValueFilePartsData.Length; i++)
                {
                    Console.Write((char)retValueFilePartsData[i]);
                    retValueString += (char)retValueFilePartsData[i];
                }

                Console.WriteLine();

                Console.WriteLine(" , After query execution , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                if ( FileTransferServerConfig.bDebug == true )
                {
                    Console.WriteLine("retValueString = " + retValueString);
                }
                Console.WriteLine("=========================================================================");

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