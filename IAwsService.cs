using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EncodingMonitor
{
    public interface IAwsService
    {
        void RenameFolder(string oldFolderPath, string newFolderPath);
        void CreateFolder(string folderPath);
        void DeleteFolder(string folderPath);
        void CopyFolder(string sourceFolderPath, string targetFolderPath);
        void DeleteFile(string fileKey);
        string UploadFile(string localFilePath, string folderPath);
        Task<string> UploadFileFromStream(Stream stream, string s3Key);
        List<string> ListObjectsInBucket(string folderKey);
        void CopyObject(string sourceKey, string destKey);
        bool CheckObjectExists(string key);

        string UploadFileFromStreamWithChunking(MemoryStream stream, string s3Key);

    }
}
