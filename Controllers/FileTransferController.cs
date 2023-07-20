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

        /// <summary>
        /// File transfer controller class & constructor.
        /// </summary>
        /// 
        /// <param name="inputLogger"> injected input logger object.</param>
        /// 
        public FileTransferController(ILogger<FileTransferController> inputLogger)
        {
            var dbConnection = new MongoClient("mongodb://localhost:27017");
            currentDB = dbConnection.GetDatabase("FileTransferDB");

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
            FileStream fileDestination;

            try
            {
                IMongoCollection<FilePartsData> currentCollection = DataHelperUtils.CreateDBCollection(currentDB, fileName);

                retValueString += "Collection has gotten created , ";

                string fileNameFQDN = FileTransferServerConfig.inputFilePath + fileName;
                string fileNameDestFQDN = FileTransferServerConfig.inputFilePath + "SITCAOutputFile.txt";

                // Create the destination file handle to write the data to
                
                fileDestination = System.IO.File.Create(fileNameDestFQDN, 10000, FileOptions.RandomAccess);
                currentFS = System.IO.File.Open(fileNameFQDN, FileMode.Open, FileAccess.ReadWrite);


                retValueString += "File is opened for Read/Write operations , ";

                // Read the file and load the data into database
                
                int totalNumberOfBytesRead = 0;
                int currentIterationCount = 0;
                int totalNumberOfFilePartsLoaded = 0;


                byte[] bytesToBeRead = new byte[FileTransferServerConfig.chunkSize];

                Int64 currentOffset = 0;
                int currentChunkNumber = 0;

                retValueString += "Bytes are being read into the stream , ";


                Console.WriteLine("=========================================================================");
                Console.WriteLine(" , Start from read stream , current time = " + DateTime.Now.Hour + ":" + 
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                int fileReadRetValue = currentFS.Read(bytesToBeRead, 0, FileTransferServerConfig.chunkSize);

                totalNumberOfBytesRead += fileReadRetValue;

                Console.WriteLine("=====================================================");

                // Loop through the file to read entire content
                
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

                    byte[] bytesToBeReadLastChunk = (currentSizeFileRead < FileTransferServerConfig.chunkSize) ?
                        LoadLastChunkData(currentSizeFileRead, bytesToBeRead) : bytesToBeRead;

                    // Add Read bytes data to mongo DB.

                    FilePartsData newFilePartsToBeAdded = DataHelperUtils.AddDataToCollection(currentIterationCount+1,
                        "File-Part-" + currentIterationCount,
                        (currentSizeFileRead < FileTransferServerConfig.chunkSize) ? bytesToBeReadLastChunk
                        : bytesToBeRead);

                    currentCollection.InsertOne(newFilePartsToBeAdded);
                    totalNumberOfFilePartsLoaded++;

                    // Write data into a pre-designated output file.

                    int currentWriteOffset = currentChunkNumber * FileTransferServerConfig.chunkSize;

                    Console.WriteLine("Current offset value = " + currentWriteOffset);

                    fileDestination.Seek(currentWriteOffset, SeekOrigin.Begin);
                    fileDestination.Write((currentSizeFileRead < FileTransferServerConfig.chunkSize) ? 
                        bytesToBeReadLastChunk : bytesToBeRead);


                    // Build debug string for console display.

                    string currentChunkStr = ConvertBytesArrayToCharString(currentSizeFileRead, bytesToBeRead, 
                        bytesToBeReadLastChunk);

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        Console.WriteLine("Current string value = " + currentChunkStr);
                    }

                    // Read next byte set of data

                    currentOffset += (Int64) ( FileTransferServerConfig.chunkSize );
                    currentChunkNumber++;

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        retValueString += " Bytes read : SubSequent Read , currentOffset = " + currentOffset +
                        "chunkSize = " + FileTransferServerConfig.chunkSize + " , fileReadRetValue = " + fileReadRetValue;
                    }

                    // Proceed to read next set of bytes in input file

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

                Console.WriteLine("=====================================================");

                Console.WriteLine(" ,Total Number of Bytes Read = " + totalNumberOfBytesRead + 
                    " , Total Number of file parts read " + totalNumberOfFilePartsLoaded);

                
                // Add Total number of Parts Data

                byte[] noOfPartsByteArray = DataHelperUtils.ConvertIntToByteArray(totalNumberOfFilePartsLoaded);

                FilePartsData numberOfFilePartsAddedData = DataHelperUtils.AddDataToCollection(currentIterationCount + 1,
                    "NumberOfFileParts", noOfPartsByteArray);

                currentCollection.InsertOne(numberOfFilePartsAddedData);


                Console.WriteLine(" , End of read stream , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                Console.WriteLine("=========================================================================");

                currentFS.Close();
                fileDestination.Close();

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
                
                IMongoCollection<FilePartsData> currentCollection = currentDB.GetCollection<FilePartsData>(fileName);


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

        /// <summary>
        /// Loads the last chunk of data of smaller length than chunk size.
        /// </summary>
        /// 
        /// <param name="currentSizeFileRead"> Number of bytes read in current stream.</param>
        /// <param name="bytesToBeRead"> Current read bytes buffer array.</param>
        /// 
        /// <returns> Loaded last chunk of data.</returns>
        byte[] LoadLastChunkData(int currentSizeFileRead, byte[] bytesToBeRead)
        {

            byte[] bytesToBeReadLastChunk = new byte[currentSizeFileRead];

            if (currentSizeFileRead < FileTransferServerConfig.chunkSize)
            {

                for (int i = 0; i < currentSizeFileRead; i++)
                {
                    bytesToBeReadLastChunk[i] = bytesToBeRead[i];
                }
            }

            return bytesToBeReadLastChunk;
        }

        /// <summary>
        /// Converts bytes array into character string.
        /// </summary>
        /// 
        /// <param name="currentSizeFileRead"> Number of bytes read in current stream.</param>
        /// <param name="bytesToBeRead"> Current read bytes buffer array.</param>
        /// <param name="bytesToBeReadLastChunk"> Last chunk of data read from the stream.</param>
        /// 
        /// <returns> Converted String Array.</returns>
        string ConvertBytesArrayToCharString(int currentSizeFileRead, byte[] bytesToBeRead, byte[] bytesToBeReadLastChunk)
        {

            string currentChunkStr = "";

            if (currentSizeFileRead < FileTransferServerConfig.chunkSize)
            {
                for (int i = 0; i < bytesToBeReadLastChunk.Length; i++)
                {
                    currentChunkStr += (char)bytesToBeReadLastChunk[i];
                }
            }
            else
            {
                for (int i = 0; i < bytesToBeRead.Length; i++)
                {
                    currentChunkStr += (char)bytesToBeRead[i];
                }
            }

            return currentChunkStr;
        }
    }
}