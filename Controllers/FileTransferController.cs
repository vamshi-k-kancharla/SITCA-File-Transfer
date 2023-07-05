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
        IMongoDatabase currentDB;

        public FileTransferController(ILogger<FileTransferController>? inputLogger)
        {
            var dbConnection = new MongoClient("mongodb://localhost:27017");
            currentDB = dbConnection.GetDatabase("FileTransferDB");

            logger = inputLogger;
        }

        [HttpGet]
        [Route("")]
        [Route("LoadFile/{fileName?}")]
        public IResult LoadFile(string? fileName)
        {

            string retValueString = "";
            FileStream currentFS = null;

            try
            {
                IMongoCollection<FilePartsData> currentCollection = CreateDBCollection(fileName);

                retValueString += "Collection has gotten created , ";

                string fileNameFQDN = FileTransferServerConfig.inputFilePath + fileName;

                currentFS = System.IO.File.Open(fileNameFQDN, FileMode.Open, FileAccess.ReadWrite);

                retValueString += "File is opened for Read/Write operations , ";

                
                int totalNumberOfBytesRead = 0;
                int currentIterationCount = 0;
                int totalNumberOfFilePartsLoaded = 0;


                byte[] bytesToBeRead = new byte[FileTransferServerConfig.chunkSize];

                Int64 currentOffset = 0;

                retValueString += "Bytes are being read into the stream , ";

                Console.WriteLine("=========================================================================");
                Console.WriteLine(" , Start from read stream , current time = " + DateTime.Now.Hour + ":" + 
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                int fileReadRetValue = currentFS.Read(bytesToBeRead, 0, FileTransferServerConfig.chunkSize);

                totalNumberOfBytesRead += fileReadRetValue;

                while (fileReadRetValue != 0)
                {
                    
                    if(FileTransferServerConfig.bDebug == true)
                    {
                        retValueString += " Bytes are read into the stream , currentOffset = " + currentOffset +
                            "chunkSize = " + FileTransferServerConfig.chunkSize + " , fileReadRetValue = " + fileReadRetValue;
                    }

                    int currentSizeFileRead = (fileReadRetValue < FileTransferServerConfig.chunkSize) ?
                        fileReadRetValue : FileTransferServerConfig.chunkSize;

                    Console.WriteLine("Current size of filePart Read = " + currentSizeFileRead);

                    // Handle Last Read separately

                    byte[] bytesToBeReadLastChunk = new byte[currentSizeFileRead];

                    if ( currentSizeFileRead < FileTransferServerConfig.chunkSize )
                    {
                        
                        for( int i = 0; i < currentSizeFileRead; i++)
                        {
                            bytesToBeReadLastChunk[i] = bytesToBeRead[i];
                        }

                    }

                    // Add bytes data to mongo DB.

                    FilePartsData newFilePartsToBeAdded = AddDataToCollection(currentIterationCount+1,
                        "File-Part-" + currentIterationCount,
                        (currentSizeFileRead < FileTransferServerConfig.chunkSize) ? bytesToBeReadLastChunk
                        : bytesToBeRead);

                    currentCollection.InsertOne(newFilePartsToBeAdded);
                    totalNumberOfFilePartsLoaded++;

                    // Read next byte set of data

                    currentOffset += (Int64) ( FileTransferServerConfig.chunkSize );

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        retValueString += " Bytes read : SubSequent Read , currentOffset = " + currentOffset +
                        "chunkSize = " + FileTransferServerConfig.chunkSize + " , fileReadRetValue = " + fileReadRetValue;
                    }

                    // Proceed to read next set of bytes

                    currentFS.Seek(currentOffset, SeekOrigin.Begin);
                    fileReadRetValue = currentFS.Read(bytesToBeRead, 0, FileTransferServerConfig.chunkSize);

                    totalNumberOfBytesRead += fileReadRetValue;

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        retValueString += " Bytes read : end of read loop , currentOffset = " + currentOffset +
                        "chunkSize = " + FileTransferServerConfig.chunkSize + " , fileReadRetValue = " + fileReadRetValue;
                    }

                    currentIterationCount++;

                }

                Console.WriteLine(" ,Total Number of Bytes Read = " + totalNumberOfBytesRead + 
                    " , Total Number of file parts read " + totalNumberOfFilePartsLoaded);

                
                // Add Total number of Parts Data

                byte[] noOfPartsByteArray = ConvertIntToByteArray(totalNumberOfFilePartsLoaded);

                FilePartsData numberOfFilePartsAddedData = AddDataToCollection(currentIterationCount + 1,
                    "NumberOfFileParts", noOfPartsByteArray);

                currentCollection.InsertOne(numberOfFilePartsAddedData);


                Console.WriteLine(" , End of read stream , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                Console.WriteLine("=========================================================================");

                currentFS.Close();

            }

            catch (Exception e)
            {

                logger.LogInformation("Exception occured while Loading the file into FilePartsData : Exception = " + e.Message);

                if(currentFS != null)
                {
                    currentFS.Close();
                }

                return Results.BadRequest("Exception occured while loading the input file  : exception = " + e.Message);
            }

            return Results.Ok(retValueString);
        }

        [HttpGet]
        [Route("GetFilePartData/{fileName?}/{filePartName?}")]
        public IResult GetFilePartData(string fileName, string filePartName)
        {

            byte[] retValueFilePartsData = new byte[FileTransferServerConfig.chunkSize];
            string retValueString = "";

            try
            {
                
                IMongoCollection<FilePartsData> currentCollection = currentDB.GetCollection<FilePartsData>(fileName);


                Console.WriteLine("=========================================================================");
                Console.WriteLine(" , Before query execution  , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                IFindFluent<FilePartsData, FilePartsData> queriedFilePartData  = 
                    currentCollection.Find(x => x.filePartName == filePartName);

                if (queriedFilePartData == null)
                {
                    Console.Write("Null data found for input query");
                    throw new ArgumentNullException("File data for the input query doesn't exist");
                }

                var filePartsDataResponse = queriedFilePartData.FirstOrDefault();

                if (filePartsDataResponse == null)
                {
                    Console.Write("Null data found for input query. After firstOrDefault ");
                    throw new ArgumentNullException("File data for the input query doesn't exist");
                }

                retValueFilePartsData = filePartsDataResponse.filePartData;

                if (retValueFilePartsData == null)
                {
                    Console.Write("Null data found for input query, after filePartData");
                    throw new ArgumentNullException("File data for the input query doesn't exist");
                }

                Console.WriteLine(" , After query execution , before processing = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                for ( int i = 0; i < retValueFilePartsData.Length; i++)
                {
                    retValueString += (char)retValueFilePartsData[i];
                }

                Console.WriteLine(" , After query execution , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                Console.WriteLine("=========================================================================");

            }

            catch (Exception e)
            {
                logger.LogInformation("Exception occured while querying the file parts Data : Exception = " + e.Message);
                return Results.BadRequest("Exception occured while querying the file parts Data  : exception = " + e.Message);
            }

            return Results.Ok(retValueFilePartsData);
        }



        // Create DB Collection

        IMongoCollection<FilePartsData> CreateDBCollection(string fileName)
        {
            currentDB.DropCollection(fileName);
            currentDB.CreateCollection(fileName);

            return currentDB.GetCollection<FilePartsData>(fileName);
        }

        FilePartsData AddDataToCollection(int inputId,string partName, byte[] partData)
        {

            FilePartsData currentFileData = new FilePartsData();

            currentFileData._id = inputId; // Random.Shared.Next(1000000);
            currentFileData.filePartName = partName;
            currentFileData.filePartData = partData;

            return currentFileData;

        }

        byte[] ConvertIntToByteArray(int inputNum)
        {

            string strTotalFilePartsLoaded = Convert.ToString(inputNum);

            byte[] noOfPartsByteArray = new byte[strTotalFilePartsLoaded.Length];

            for ( int i = 0; i < strTotalFilePartsLoaded.Length; i++ )
            {

                noOfPartsByteArray[i] = (byte)strTotalFilePartsLoaded[i];
            }

            return noOfPartsByteArray;

            /*

            byte[] noOfPartsByteArray = new byte[4];

            noOfPartsByteArray[0] =  (byte)( ( inputNum & 0xFF000000 ) >> 6 );
            noOfPartsByteArray[1] = (byte)((inputNum & 0x00FF0000) >> 4);
            noOfPartsByteArray[2] = (byte)((inputNum & 0x0000FF00) >> 2);
            noOfPartsByteArray[3] = (byte)(inputNum & 0x000000FF);

            return noOfPartsByteArray;

            */
        }

    }

}