
using MongoDB.Driver;

namespace SITCAFileTransferService.Common
{
    public class DataHelperUtils
    {

        /// <summary>
        /// Recreates file collection to be used for data manipulations.
        /// </summary>
        /// 
        /// <param name="currentDB"> IMongoDVB reference to take care of CRUD operations.</param>
        /// <param name="fileName"> Name of the input file to create the collection for.</param>
        /// 
        /// <returns> A collection ref of created file collection.</returns>

        public static IMongoCollection<FilePartsData> CreateDBCollection(IMongoDatabase currentDB, string fileName)
        {
            currentDB.DropCollection(fileName);
            currentDB.CreateCollection(fileName);

            return currentDB.GetCollection<FilePartsData>(fileName);
        }


        /// <summary>
        /// Creates FilePartsData object and fills it with input values.
        /// </summary>
        /// 
        /// <param name="inputId"> Id of object to be added.</param>
        /// <param name="partName"> Name of file part to be added to database.</param>
        /// <param name="partData"> Content of the file part to be added to database.</param>
        /// 
        /// <returns> FileParts data object that's built from input Values.</returns>

        public static FilePartsData AddDataToCollection(int inputId, string partName, byte[] partData)
        {

            FilePartsData currentFileData = new FilePartsData();

            currentFileData._id = inputId;

            currentFileData.filePartName = partName;
            currentFileData.filePartData = partData;

            return currentFileData;

        }


        /// <summary>
        /// Converts Input integer into bytes array.
        /// </summary>
        /// 
        /// <param name="inputNum"> Input integer that gets converted to byte Array.</param>
        /// 
        /// <returns> converted byte array data from input integer.</returns>

        public static byte[] ConvertIntToByteArray(int inputNum)
        {

            string strTotalFilePartsLoaded = Convert.ToString(inputNum);

            byte[] noOfPartsByteArray = new byte[strTotalFilePartsLoaded.Length];

            for (int i = 0; i < strTotalFilePartsLoaded.Length; i++)
            {

                noOfPartsByteArray[i] = (byte)strTotalFilePartsLoaded[i];
            }

            return noOfPartsByteArray;
        }

    }
}