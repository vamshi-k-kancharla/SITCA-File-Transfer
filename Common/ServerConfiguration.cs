
namespace SITCAFileTransferService.Common
{
    /// <summary>
    /// Configuration values of SITCA file transfer server.
    /// </summary>

    public class FileTransferServerConfig
    {
        public static string inputFilePath = @"I:\SITCA File Transfer\SITCA Web Service\SourceDirectory\";
        
        public static int chunkSize = 100000;

        public static int fileSize = 100000000;

        public static bool bDebug = false;
    }
}