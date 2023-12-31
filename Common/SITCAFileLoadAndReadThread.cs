using MongoDB.Driver;

namespace SITCAFileTransferService.Common
{
    public class SITCAFileLoadAndReadThread
    {

        /// <summary>
        /// A parallel thread that reads chunk of data from the file into database                                                                                              r.
        /// </summary>
        /// 
        /// <param name="fileLoadReadParamObj"> Thread initiation parameters passed through object. </param>
        /// 
        /// <returns> None.</returns>

        public static void FileLoadAndReadThread(object fileLoadReadParamObj)
        {

            string retValueString = "";
            FileStream currentFS = null;
            bool isTheMutexLocked = false;

            try
            {
                IMongoDatabase currentDB = ((LoadThreadObject)fileLoadReadParamObj).currentDB;
                string fileName = ((LoadThreadObject)fileLoadReadParamObj).fileName;
                long currentOffset = ((LoadThreadObject)fileLoadReadParamObj).currentOffset;
                long startPart = ((LoadThreadObject)fileLoadReadParamObj).startPart;
                long numberOfSubParts = ((LoadThreadObject)fileLoadReadParamObj).numOfSubParts;

                IMongoCollection<FilePartsData> currentCollection = ((LoadThreadObject)fileLoadReadParamObj).currentCollection;
                currentFS = ((LoadThreadObject)fileLoadReadParamObj).currentFS;


                long currentIterationCount = 0; // ((LoadThreadObject)fileLoadReadParamObj).currentIterationCount;

                for (long currentThreadPart = startPart; currentThreadPart < startPart + numberOfSubParts;
                    currentThreadPart++)
                {

                    currentIterationCount = currentThreadPart;
                    currentOffset = currentThreadPart * FileTransferServerConfig.chunkSize;

                    if (FileTransferServerConfig.bFirstLevelDebug == true)
                    {

                        Console.WriteLine("Thread spinned with below context => fileName = " + fileName +
                        " ,currentOffset = " + currentOffset + " ,currentIterationCount = " + currentIterationCount);
                    }

                    // Start processing the request 

                    retValueString += "Collection has gotten created , ";

                    byte[] bytesToBeRead = new byte[FileTransferServerConfig.chunkSize];

                    retValueString += "Bytes are being read into the stream , ";

                    if (FileTransferServerConfig.bFirstLevelDebug == true)
                    {

                        Console.WriteLine("=========================================================================");
                        Console.WriteLine(" , Start from read stream , current time = " + DateTime.Now.Hour + ":" +
                            DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);
                    }

                    long fileReadRetValue = RandomAccess.Read(currentFS.SafeFileHandle, bytesToBeRead, currentOffset);

                    if (FileTransferServerConfig.bFirstLevelDebug == true)
                    {

                        Console.WriteLine("=====================================================");
                    }

                    long currentSizeFileRead = (fileReadRetValue < FileTransferServerConfig.chunkSize) ?
                        fileReadRetValue : FileTransferServerConfig.chunkSize;

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        Console.WriteLine("SITCAFileLoadAndReadThread: Current size of filePart Read = " + currentSizeFileRead);
                    }

                    // Handle Last Read separately

                    byte[] bytesToBeReadLastChunk = (currentSizeFileRead < FileTransferServerConfig.chunkSize) ?
                        LoadLastChunkData(currentSizeFileRead, bytesToBeRead) : bytesToBeRead;

                    // Add Read bytes data to mongo DB.

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        Console.WriteLine("SITCAFileLoadAndReadThread : data is being written into database collection");
                    }

                    FilePartsData newFilePartsToBeAdded = DataHelperUtils.AddDataToCollection((int)(currentIterationCount + 1),
                        "File-Part-" + currentIterationCount,
                        (currentSizeFileRead < FileTransferServerConfig.chunkSize) ? bytesToBeReadLastChunk
                        : bytesToBeRead);

                    currentCollection.InsertOne(newFilePartsToBeAdded);

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        Console.WriteLine("SITCAFileLoadAndReadThread : data has been written into DB Collection");
                    }

                    // Build debug string for console display.

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        string currentChunkStr = DataHelperUtils.ConvertBytesArrayToCharString((int)currentSizeFileRead,
                            bytesToBeRead, bytesToBeReadLastChunk);

                        Console.WriteLine("Current string value = " + currentChunkStr);
                    }

                    if (FileTransferServerConfig.bDebug == true)
                    {
                        retValueString += " Bytes read : SubSequent Read , currentOffset = " + currentOffset +
                        "chunkSize = " + FileTransferServerConfig.chunkSize + " , fileReadRetValue = " + fileReadRetValue;
                    }

                    if (FileTransferServerConfig.bFirstLevelDebug == true)
                    {

                        Console.WriteLine("=====================================================");

                        Console.WriteLine(" , End of read stream , current time = " + DateTime.Now.Hour + ":" +
                            DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);

                        Console.WriteLine("=========================================================================");
                    }
                }

            }

            catch (Exception e)
            {

                if (currentFS != null)
                {
                    currentFS.Close();
                }

                Console.WriteLine("Some error occured while reading input file data : " + e.Message);

            }

        }

        /// <summary>
        /// Loads the last chunk of data of smaller length than chunk size.
        /// </summary>
        /// 
        /// <param name="currentSizeFileRead"> Number of bytes read in current stream.</param>
        /// <param name="bytesToBeRead"> Current read bytes buffer array.</param>
        /// 
        /// <returns> Loaded last chunk of data.</returns>
        private static byte[] LoadLastChunkData(long currentSizeFileRead, byte[] bytesToBeRead)
        {

            byte[] bytesToBeReadLastChunk = new byte[currentSizeFileRead];

            if (currentSizeFileRead < FileTransferServerConfig.chunkSize)
            {

                for (long i = 0; i < currentSizeFileRead; i++)
                {
                    bytesToBeReadLastChunk[i] = bytesToBeRead[i];
                }
            }

            return bytesToBeReadLastChunk;
        }

        /// <summary>
        /// Checks whether all the threads supplied to it are stopped.
        /// </summary>
        /// 
        /// <param name="threadList"> List of all the threads whose status need to be checked for.</param>
        /// 
        /// <returns> A boolean with Yes ( for all stopped ) & No ( not all threads stopped ) values.</returns>

        static public bool AreAllThreadsStopped(List<Thread> threadList)
        {
            long i = 0;

            for (; i < threadList.Count; i++)
            {

                if (threadList[(int)i].ThreadState == ThreadState.Running)
                {
                    break;
                }
            }

            if (i == threadList.Count)
            {
                return true;
            }

            return false;
        }

    }

}