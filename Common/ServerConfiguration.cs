
namespace SITCAFileTransferService.Common
{
    /// <summary>
    /// Configuration values of SITCA file transfer server.
    /// </summary>

    public class FileTransferServerConfig
    {
        public static string inputFilePath = @"I:\SITCA File Transfer\SITCA Web Service\SourceDirectory\";
        
        

        public static int chunkSize = 10000000;

        public static long fileSize = 2000000000;

        public static long numberOfThreads = 40;



        public static bool bFirstLevelDebug = false;

        public static bool bDebug = false;

        public static Mutex readThreadSyncMutex = new Mutex();
    }
}