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

        [HttpPost]
        [Route("")]
        [Route("LoadFile/{fileName?}")]
        public IResult LoadFile(string? fileName)
        {

            string retValueString = "";

            try
            {
                IMongoCollection<FilePartsData> currentCollection = CreateDBCollection(fileName);

                retValueString += "Collection has gotten created , ";

                string fileNameFQDN = FileTransferServerConfig.inputFilePath + fileName;

                FileStream currentFS = System.IO.File.Open(fileNameFQDN, FileMode.Open, FileAccess.ReadWrite);

                retValueString += "File is opened for Read/Write operations , ";

                
                int totalNumberOfBytesRead = 0;


                byte[] bytesToBeRead = new byte[FileTransferServerConfig.chunkSize];

                int currentOffset = 0;

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


                    /*
                    for (int i = 0; i < currentSizeFileRead; i++)
                    {
                        retValueString += (char)bytesToBeRead[i];
                    }*/

                    currentOffset += FileTransferServerConfig.chunkSize;

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        retValueString += " Bytes read : Sub Sequent Read , currentOffset = " + currentOffset +
                        "chunkSize = " + FileTransferServerConfig.chunkSize + " , fileReadRetValue = " + fileReadRetValue;
                    }

                    currentFS.Seek(currentOffset, SeekOrigin.Begin);
                    fileReadRetValue = currentFS.Read(bytesToBeRead, 0, FileTransferServerConfig.chunkSize);

                    totalNumberOfBytesRead += fileReadRetValue;

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        retValueString += " Bytes read : end of read loop , currentOffset = " + currentOffset +
                        "chunkSize = " + FileTransferServerConfig.chunkSize + " , fileReadRetValue = " + fileReadRetValue;
                    }

                }

                Console.WriteLine(" ,Total Number of Bytes Read = " + totalNumberOfBytesRead);

                Console.WriteLine(" , End of read stream , current time = " + DateTime.Now.Hour + ":" +
                    DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

            }

            catch (Exception e)
            {

                logger.LogInformation("Exception occured while Loading the file into FilePartsData : Exception = " + e.Message);
            }

            return Results.Ok(retValueString);
        }


        // Create DB Collection

        IMongoCollection<FilePartsData> CreateDBCollection(string fileName)
        {
            currentDB.DropCollection(fileName);
            currentDB.CreateCollection(fileName);

            return currentDB.GetCollection<FilePartsData>(fileName);
        }

    }

}