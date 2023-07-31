
namespace SITCAFileTransferService.Common
{
    /// <summary>
    /// Configuration values of SITCA file transfer server.
    /// </summary>

    public class FileTransferServerConfig
    {
        public static string inputFilePath = @"I:\SITCA File Transfer\SITCA Web Service\SourceDirectory\";
        public static int chunkSize = 25;

        public static bool bDebug = false;
    }
}