
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

        public long currentOffset;

        public long currentIterationCount;

        public IMongoCollection<FilePartsData> currentCollection;

        public FileStream currentFS;

        public long startPart;

        public long numOfSubParts;

    }

}
