
using MongoDB.Driver;

namespace SITCAFileTransferService
{
    public class FilePartsDataPoc
    {
        public int _id;

        public string? filePartName;

        public string? filePartData;

    }

    public class FilePartsData
    {
        public int _id;

        public string? filePartName;

        public byte[]? filePartData;

    }

    public class LoadThreadObject
    {
        public IMongoDatabase currentDB;

        public string fileName;

        public int currentOffset;

        public int currentIterationCount;

        public IMongoCollection<FilePartsData> currentCollection;
    }

}
