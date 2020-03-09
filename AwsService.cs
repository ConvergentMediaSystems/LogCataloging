using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
//using Ecp.Configuration.Resources;
using Microsoft.Extensions.Configuration;
using Amazon;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace EncodingMonitor
{
    public class AwsService  //:IAwsService
    {
        private readonly IAmazonS3 _awsS3Client;

        private readonly string _bucketName;
        //private readonly string _awsEndpoint;
        private string bucketUrl;
        

        public AwsService(string awsAccessKey, string awsSecretAccesskey, string bucketName)
        {
            /*
            var awsAccesskey = configurationManager.GetSection("s3").GetSection("accessKey").Value;
            var awsSecretAccesskey = configurationManager.GetSection("s3").GetSection("secretAccessKey").Value;
            _bucketName = configurationManager.GetSection("s3").GetSection("bucket").Value;
            //_awsEndpoint = configurationManager.GetSection("s3").GetSection("s3UrlPrefix").Value;
            */
            _awsS3Client = new AmazonS3Client(awsAccessKey, awsSecretAccesskey, RegionEndpoint.USEast1);
            _bucketName = bucketName;
            
            bucketUrl = "https://" + _bucketName + ".s3.amazonaws.com/";
        }

        public void RenameFolder(string oldFolderPath, string newFolderPath)
        {
            try
            {
                CopyFolder(oldFolderPath, newFolderPath);
                DeleteFolder(oldFolderPath);
            }
            catch (Exception ex)
            {
                throw new ApplicationException("S3 RenameFolder - unexpected exception.", ex);
            }
        }

        public bool CheckObjectExists(string key)
        {
            bool exists = false;

            try
            {
                GetObjectMetadataRequest req = new GetObjectMetadataRequest();
                req.BucketName = this._bucketName;
                req.Key = key.Replace(bucketUrl,"");

                GetObjectMetadataResponse res = _awsS3Client.GetObjectMetadata(req);

                if(res.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    exists = true;
                } 
            }
            catch (AmazonS3Exception s3Ex)
            {
                if(s3Ex.ErrorCode != "NotFound")
                {
                    throw s3Ex;
                } else
                {
                    exists = false;
                }
            }
            catch(Exception ex)
            {
                throw ex;
            }

            return exists;
        }

        public long CheckFileSize(string key)
        {
            long size = -1;

            try
            {
                
                GetObjectMetadataRequest req = new GetObjectMetadataRequest();
                req.BucketName = this._bucketName;
                req.Key = key.Replace(bucketUrl, "");

                GetObjectMetadataResponse res = _awsS3Client.GetObjectMetadata(req);

                if (res.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    size = res.ContentLength;
                }
            }
            catch (AmazonS3Exception s3Ex)
            {
                if (s3Ex.ErrorCode != "NotFound")
                {                    
                    throw s3Ex;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return size;
        }



        public void CreateFolder(string folderPath)
        {
            try
            {
                folderPath = CorrectFolderPath(folderPath);

                var request = new PutObjectRequest
                {
                    BucketName = _bucketName,
                    Key = folderPath,
                    InputStream = new MemoryStream()
                };

                var response =  _awsS3Client.PutObject(request);

                if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
                {
                    throw new ApplicationException("CreateFolder(" + folderPath + ") returned status code " + response.HttpStatusCode.ToString());
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException("CreateFolder - unexpected exception.", ex);
            }
        }

        public void DeleteFolder(string folderPath)
        {
            try
            {
                folderPath = CorrectFolderPath(folderPath);

                // get entire contents of source folder
                ListObjectsRequest listRequest = new ListObjectsRequest()
                {
                    BucketName = _bucketName,
                    Prefix = folderPath
                };
                ListObjectsResponse listResponse = _awsS3Client.ListObjects(listRequest);

                List<KeyVersion> objectKeys = new List<KeyVersion>();
                foreach (var s3Object in listResponse.S3Objects)
                {
                    objectKeys.Add(new KeyVersion { Key = s3Object.Key });
                }

                // empty folders do not really exist in S3 so don't try to delete an empty folder
                if (objectKeys.Count > 0)
                {
                    DeleteObjectsRequest request = new DeleteObjectsRequest
                    {
                        Objects = objectKeys,
                        BucketName = _bucketName
                    };

                    DeleteObjectsResponse response = _awsS3Client.DeleteObjects(request);
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException("S3 DeleteFolder - unexpected exception.", ex);
            }
        }

        public void CopyFolder(string sourceFolderPath, string targetFolderPath)
        {
            try
            {
                targetFolderPath = CorrectFolderPath(targetFolderPath);
                sourceFolderPath = CorrectFolderPath(sourceFolderPath);

                if (targetFolderPath == sourceFolderPath)
                {
                    throw new ApplicationException("CopyFolder: target and source paths cannot be equal.");
                }

                // get entire contents of source folder
                ListObjectsRequest listRequest = new ListObjectsRequest()
                {
                    BucketName = _bucketName,
                    Prefix = sourceFolderPath
                };
                ListObjectsResponse listResponse = _awsS3Client.ListObjects(listRequest);

                // create empty target folder
                CreateFolder(targetFolderPath);

                // copy contents of source to dest
                foreach (var sourceObject in listResponse.S3Objects)
                {
                    CopyObjectRequest request = new CopyObjectRequest
                    {
                        SourceKey = sourceObject.Key,
                        DestinationKey = sourceObject.Key.Replace(sourceFolderPath, targetFolderPath),
                        SourceBucket = _bucketName,
                        DestinationBucket = _bucketName,
                    };

                    CopyObjectResponse response = _awsS3Client.CopyObject(request);
                }

            }
            catch (Exception ex)
            {
                throw new ApplicationException("S3 CopyFolder - unexpected exception.", ex);
            }
        }

        public void CopyObject(string sourceKey, string destKey)
        {
            try
            {
                CopyObjectRequest request = new CopyObjectRequest
                {
                    SourceKey = sourceKey,
                    DestinationKey = destKey,
                    SourceBucket = _bucketName,
                    DestinationBucket = _bucketName,
                };

                CopyObjectResponse response = _awsS3Client.CopyObject(request);
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }

        public void DeleteFile(string fileKey)
        {
            try
            {
                DeleteObjectRequest request = new DeleteObjectRequest
                {
                    Key = fileKey,
                    BucketName = _bucketName
                };

                DeleteObjectResponse response = _awsS3Client.DeleteObject(request);
                if (response.HttpStatusCode != System.Net.HttpStatusCode.NoContent)
                {
                    throw new ApplicationException("S3 DeleteFile - received status code " + response.HttpStatusCode.ToString() + ".");
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException("S3 DeleteFile - unexpected exception.", ex);
            }
        }

        public string UploadFile(string localFilePath, string s3Folder)
        {
            string uploadedFileUrl = string.Empty;

            try
            {
                s3Folder = CorrectFolderPath(s3Folder);

                string key = s3Folder + Path.GetFileName(localFilePath);

                using (var transferUtility = new Amazon.S3.Transfer.TransferUtility(_awsS3Client))
                {
                    using (FileStream fs = new FileStream(localFilePath, FileMode.Open))
                    {
                        var request = new TransferUtilityUploadRequest
                        {
                            BucketName = _bucketName,
                            Key = key,
                            InputStream = fs,
                            CannedACL = S3CannedACL.PublicRead,
                        };

                        transferUtility.Upload(request);
                    }

                    uploadedFileUrl = bucketUrl + key;
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException("S3 UploadfileToS3 - unexpected exception.", ex);
            }

            return uploadedFileUrl;
        }

        public List<string> ListObjectsInBucket(string folderKey)
        {
            List<string> keys = new List<string>();

            try
            {
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = _bucketName;
                request.Prefix = folderKey;
                ListObjectsResponse response = _awsS3Client.ListObjects(request);
                foreach (S3Object o in response.S3Objects)
                {
                    keys.Add(o.Key);
                }
            }
            catch(Exception ex)
            {
                throw new ApplicationException("S3 ListObjectsInBucket ", ex);
            }

            return keys;
        }

        public async Task<string> UploadFileFromStream(Stream stream, string s3Key)
        {
            string uploadedFileUrl = string.Empty;

            try
            {
                using (var transferUtility = new Amazon.S3.Transfer.TransferUtility(_awsS3Client))
                {
                    using (stream)
                    {
                        var request = new TransferUtilityUploadRequest
                        {
                            BucketName = _bucketName,
                            Key = s3Key,
                            InputStream = stream,
                            CannedACL = S3CannedACL.PublicRead,
                        };

                        await transferUtility.UploadAsync(request);
                    }

                    uploadedFileUrl = bucketUrl + s3Key;
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException("S3 UploadFileFromStream - unexpected exception.", ex);
            }

            return uploadedFileUrl;
        }

        public string UploadFileFromStreamWithChunking(MemoryStream stream, string s3Key)
        {
            string uploadedFileUrl = string.Empty;
            Dictionary<int, string> partNumbersAndETags = new Dictionary<int, string>();
            int chunkSize = 5500000;

            try
            {
                stream.Seek(0, SeekOrigin.Begin);
                string uploadId = string.Empty;
                int numChunks = (int)Math.Floor((double)stream.Length / chunkSize);
                if (stream.Length % chunkSize > 0)
                {
                    numChunks += 1;
                }

                byte[] chunkBytes = new byte[5500000];
                int bytesRead = 0;
                int offset = 0;
                int partNumber = 0;

                for (int i = 0; i < numChunks; i++)
                {
                    bytesRead = stream.Read(chunkBytes, 0, 5500000);
                    offset = bytesRead;

                    partNumber = i + 1;


                    if (partNumber == 1)
                    {
                        // if first chunk, initiate the upload
                        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest
                        {
                            BucketName = _bucketName,
                            Key = s3Key
                        };

                        InitiateMultipartUploadResponse initResponse = _awsS3Client.InitiateMultipartUpload(initRequest);
                        if (initResponse.HttpStatusCode != System.Net.HttpStatusCode.OK)
                        {
                            throw new ApplicationException("S3 UploadFileChunk: could not initiate multipart upload.");
                        }

                        uploadId = initResponse.UploadId;
                    }

                    Array.Resize(ref chunkBytes, bytesRead);

                    MemoryStream chunkMemoryStream = new MemoryStream(chunkBytes);

                    // pload the chunk
                    UploadPartRequest uploadRequest = new UploadPartRequest
                    {
                        BucketName = _bucketName,
                        Key = s3Key,
                        UploadId = uploadId,
                        PartNumber = partNumber,
                        PartSize = chunkBytes.Length,
                        FilePosition = offset,
                        InputStream = chunkMemoryStream
                    };

                    UploadPartResponse uploadResponse = _awsS3Client.UploadPart(uploadRequest);

                    if (uploadResponse.HttpStatusCode != System.Net.HttpStatusCode.OK)
                    {
                        AbortChunkUpload(uploadId, s3Key);
                    }

                    // get ETag and Part # from response and store it in the file information
                    partNumbersAndETags.Add(uploadResponse.PartNumber, uploadResponse.ETag);

                    // update file position
                    offset = chunkBytes.Length;
                }

                List<PartETag> partETags = new List<PartETag>();
                foreach (KeyValuePair<int, string> kvp in partNumbersAndETags)
                {
                    partETags.Add(new PartETag { ETag = kvp.Value, PartNumber = kvp.Key });
                }

                CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest
                {
                    Key = s3Key,
                    BucketName = _bucketName,
                    PartETags = partETags,
                    UploadId = uploadId
                };

                CompleteMultipartUploadResponse response = _awsS3Client.CompleteMultipartUpload(request);

                uploadedFileUrl = bucketUrl + s3Key;
            }
            catch (Exception ex)
            {
                throw new ApplicationException("UploadFileFromStream - unexpected exception.", ex);
            }

            return uploadedFileUrl;
        }

        private void AbortChunkUpload(string uploadId, string s3Key)
        {
            try
            {
                AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest
                {
                    UploadId = uploadId,
                    BucketName = _bucketName,
                    Key = s3Key
                };

                AbortMultipartUploadResponse abortResponse = _awsS3Client.AbortMultipartUpload(abortRequest);
            }
            catch (Exception) { }
        }

        /// <summary>
        /// Helper method for ensuring folder paths end in the / character.
        /// </summary>
        /// <param name="folderPath"></param>
        /// <returns></returns>
        public static string CorrectFolderPath(string folderPath)
        {
            if (!folderPath.EndsWith("/"))
            {
                folderPath += "/";
            }

            return folderPath;
        }
    }
}
