using Microsoft.AspNetCore.Mvc;
using MongoDB.Driver;
using System.Collections.Generic;

namespace SITCAFileTransferService.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class FileTransfer_POC_Controller : ControllerBase
    {
        private readonly ILogger<FileTransfer_POC_Controller>? _logger;
        IMongoDatabase currentDB;

        public FileTransfer_POC_Controller()
        {
            var dbConnection = new MongoClient("mongodb://localhost:27017");
            currentDB = dbConnection.GetDatabase("FileTransferDB");
        }

        [HttpPost]
        [Route("")]
        [Route("LoadFile/{input1?}/{input2?}")]
        public IResult LoadFile(int? input1, string? input2)
        {

            IMongoCollection<FilePartsDataPoc> currentCollection = CreateDBCollection();

            FilePartsDataPoc currentFileData = AddDataToCollection(100, "500MB_Part1", "This is 500 MB File Data of First Part");
            currentCollection.InsertOne(currentFileData);

            FilePartsDataPoc currentFileData1 = AddDataToCollection(200, "500MB_Part2", "This is 500 MB File Data of Second Part");
            currentCollection.InsertOne(currentFileData1);

            string collectionNamesString = RetriveCollectionNamesString();

            // Retrieve Collection Data

            var query = Builders<FilePartsDataPoc>.Filter.Empty;
            var currentDocuments = currentCollection.Find(query);

            var queriedDocument = currentCollection.Find(x => x.filePartName == "500MB_Part1");

            // Document Count

            var currentDocumentCount_BeforeDeletion = currentDocuments.CountDocuments();
            var currentQueriedDocumentCount_BeforeDeletion = queriedDocument.CountDocuments();

            // Retrieve Document data

            var collectionDataParts = RetrieveCollectionPartsData(currentDocuments);

            // Delete Collection Data

            currentCollection.DeleteOne(x => x.filePartName == "500MB_Part1");
            var currentDocuments_AfterDelete = currentCollection.Find(query);


            // Update Collection Data Document

            var updateDocument = Builders<FilePartsDataPoc>.Update.Set(x => x.filePartData, "This is updated File Part");
            currentCollection.UpdateOne(x => x.filePartName == "500MB_Part2", updateDocument);

            var currentDocuments_AfterUpdate = currentCollection.Find(query);

            // Retrieve Collection Document data

            var collectionDataParts_AfterUpdate = RetrieveCollectionPartsData(currentDocuments_AfterUpdate);


            string retValueString = "Loading the file from storage drive..Entered input = " + input1 + " , second = " + input2 + "            " +
                ", Total Document Data count = " + currentDocumentCount_BeforeDeletion + "            " +
                ", Queried Document Data count = " + currentQueriedDocumentCount_BeforeDeletion + "            " +
                ", Total Documents File Data = " + collectionDataParts + "            " +
                ", Number of documents after deletion = " + currentDocuments_AfterDelete.CountDocuments() + "            " +
                ", Total Documents File Data After Deletion and updation = " + collectionDataParts_AfterUpdate + "            " +
                ", DB String = " + currentDB.ToString() + "            " +
                ", List of Collections :=: " + collectionNamesString;


            return Results.Ok(retValueString);
        }


        // Create DB Collection

        IMongoCollection<FilePartsDataPoc> CreateDBCollection()
        {
            currentDB.DropCollection("500MB_File");
            currentDB.CreateCollection("500MB_File");

            return currentDB.GetCollection<FilePartsDataPoc>("500MB_File");
        }

        // Insert Documents into Collection

        // ToDo : Move all the helper functions to Common folder.

        FilePartsDataPoc AddDataToCollection(int id, string partName, string partData)
        {

            FilePartsDataPoc currentFileData = new FilePartsDataPoc();

            currentFileData._id = id;
            currentFileData.filePartName = partName;
            currentFileData.filePartData = partData;

            return currentFileData;

        }

        // Retrieve Collection Names

        string RetriveCollectionNamesString()
        {

            var newCollectionCursor = currentDB.ListCollectionNames();
            string collectionNamesString = "";

            foreach (var collectionName in newCollectionCursor.ToList())
            {
                collectionNamesString += collectionName + " ";
            }

            return collectionNamesString;
        }

        // Retrieve File Parts Data from Queried documents

        string RetrieveCollectionPartsData(IFindFluent<FilePartsDataPoc, FilePartsDataPoc> currentDocuments)
        {

            var queryDocumentCursor = currentDocuments.ToCursor();

            string collectionDataParts = "";

            foreach (var currentDoc in queryDocumentCursor.ToList<FilePartsDataPoc>())
            {
                collectionDataParts += currentDoc.filePartData + " , ";

            }

            return collectionDataParts;
        }

}

}