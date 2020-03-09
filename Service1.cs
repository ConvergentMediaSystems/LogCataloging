using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using System.Data;
using System.Data.SqlClient;

namespace EncodingMonitor
{
    public partial class EncodingMonitor : ServiceBase
    {
        int _frequency;
        string _thumbnail_suffix;
        private System.Threading.Timer timer1;
        private bool Working = false;

        string _connectionString;

        SqlConnection conn;
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        string _awsAccessKey;
        string _awsSecretAccesskey;
        string _awsBucket;

        AwsService _s3Service;

        private class Item
        {
            public int ItemId;
            public string UnprocessedName;
            public string ThumbnailName;
        }

        private class RunInfo
        {
            public string GetSPName;
            public string UpdateSPName;
            public string OriginalPath;
            public string EncodedPath;
        }

        public EncodingMonitor()
        {
            InitializeComponent();

            SqlConnectionStringBuilder connStringBuilder = new SqlConnectionStringBuilder();
            connStringBuilder.DataSource = ConfigurationManager.AppSettings["DBServerName"].ToString();
            connStringBuilder.UserID = ConfigurationManager.AppSettings["DBUserName"].ToString();
            connStringBuilder.Password = ConfigurationManager.AppSettings["DBUserPassword"].ToString();
            connStringBuilder.InitialCatalog = ConfigurationManager.AppSettings["DatabaseName"].ToString();
            _connectionString = connStringBuilder.ConnectionString;
            _awsAccessKey = ConfigurationManager.AppSettings["awsAccessKey"].ToString();
            _awsSecretAccesskey = ConfigurationManager.AppSettings["awsSecretAccesskey"].ToString();
            _awsBucket = ConfigurationManager.AppSettings["awsBucket"].ToString();
            _thumbnail_suffix = ConfigurationManager.AppSettings["_thumbnail_suffix"].ToString();

            conn = new SqlConnection(_connectionString);

            _frequency = int.Parse(ConfigurationManager.AppSettings["Frequency"].ToString());
            _s3Service = new AwsService(_awsAccessKey, _awsSecretAccesskey, _awsBucket);

            log.Info("Initiating service with following parameters:");
            log.Info("Database connection string: " + _connectionString);
            log.Info("Frequency: " + _frequency.ToString());
        }

        protected override void OnStart(string[] args)
        {

            log.Info("Starting service");
            TimeSpan tsInterval = new TimeSpan(0, 0, _frequency);
            timer1 = new System.Threading.Timer(new System.Threading.TimerCallback(timer1_Tick), null, tsInterval, tsInterval);

        }

        protected override void OnStop()
        {
            log.Info("Stoping service");

        }

        private void timer1_Tick(object state)
        {
            // Prevent starting another process while previous one not stopped yet...
            if (Working)
                return;

            Working = true;
            DateTime start = DateTime.Now;
            List<RunInfo> configs = new List<RunInfo>();

            try
            {
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }
                log.Info("Getting configuration information");
                SqlCommand cmd2 = new SqlCommand("SELECT * FROM [EncodingServiceConfig]", conn);
                cmd2.CommandType = CommandType.Text;

                using (SqlDataReader reader = cmd2.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        RunInfo ri = new RunInfo();
                        ri.GetSPName = reader.GetString(reader.GetOrdinal("GetSPName"));
                        ri.UpdateSPName = reader.GetString(reader.GetOrdinal("UpdateSPName"));
                        ri.OriginalPath = reader.GetString(reader.GetOrdinal("OriginalPath"));
                        ri.EncodedPath = reader.GetString(reader.GetOrdinal("EncodedPath"));
                        configs.Add(ri);
                    }
                }

                conn.Close();

                foreach (RunInfo ri in configs)
                {
                    log.Info("Getting list of files using SP " + ri.GetSPName);
                    List<Item> items = new List<Item>();

                    // check if last connection was not closed properly...
                    if (conn.State != ConnectionState.Open)
                    {
                        conn.Open();
                    }
                    SqlCommand cmd = new SqlCommand(ri.GetSPName, conn);
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Item it = new Item();
                            it.UnprocessedName = reader.GetString(reader.GetOrdinal("S3URL"));
                            it.ItemId = reader.GetInt32(reader.GetOrdinal("LibraryItemId"));
                            it.ThumbnailName = reader.GetString(reader.GetOrdinal("ThumbnailS3URL"));
                            items.Add(it);
                        }
                        log.Info("# of Retreived items: " + items.Count().ToString());
                    }

                    foreach (Item it in items)
                    {
                        log.Info("Processing file " + it.UnprocessedName);
                        bool IsVideo = (it.UnprocessedName.Substring(it.UnprocessedName.LastIndexOf(".") + 1).ToLower() == "mp4");

                        // check for existance
                        string EncodedFileName = "";
                        string EncodedThumbnailName = "";
                        long FileSize = 0;
                        if (ProcessFile(it.UnprocessedName, IsVideo, out EncodedFileName, out EncodedThumbnailName, out FileSize, ri.OriginalPath, ri.EncodedPath, _thumbnail_suffix))
                        {
                            // update record in database
                            log.Info("Updating record for file: " + EncodedFileName);
                            UpdateRecord(ri.UpdateSPName, it.ItemId, EncodedFileName, EncodedThumbnailName, FileSize);
                        }
                    }
                }
            }

            catch (Exception ex)
            {
                log.Error("Exception " + ex.Message);
                return;
            }

            TimeSpan span = DateTime.Now - start;
            int ms = (int)span.TotalMilliseconds;
            log.Info("Completed in " +ms.ToString() + " ms");
            Working = false;
        }


        private void UpdateRecord(string SPName, int ItemID, string EncodedFileName, string EncodedThumbnailName, long Size)
        {

            try
            {
                SqlCommand cmd = new SqlCommand(SPName, conn);
                cmd.CommandType = CommandType.StoredProcedure;

                cmd.Parameters.Add(new SqlParameter("@ItemId", ItemID));
                cmd.Parameters.Add(new SqlParameter("@EncodedFileName", EncodedFileName));
                cmd.Parameters.Add(new SqlParameter("@EncodedThumbnailName", EncodedThumbnailName));
                cmd.Parameters.Add(new SqlParameter("@FileSize", Size));

                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                log.Info("Exception while executing SP: " + SPName + " Exception: " + ex.Message);
            }

        }

        private bool ProcessFile(string FileName, bool isVideo, out string EncodedFileName, out string EncodedThumbnal, out long FileSize, string OriginalPath, string EncodedPath, string thumbnailSuffix)
        {
            string unprocessedKey = FileName;
            EncodedFileName = "";
            EncodedThumbnal = "";
            FileSize = 0;

            try
            {
                //unprocessedKey = unprocessedKey.Substring(unprocessedKey.IndexOf("brtv-playlist-manager-playlists"));
                EncodedFileName = unprocessedKey.Replace(OriginalPath, EncodedPath);
                EncodedFileName = EncodedFileName.Replace("%20", " ");
                if (!isVideo)
                {
                    EncodedFileName = EncodedFileName.Replace(".png", ".jpg");
                }

                log.Info("Checking existance of file: " + EncodedFileName);
                if (!_s3Service.CheckObjectExists(EncodedFileName))
                //if (!_s3Service.CheckObjectExists(FileName))
                {
                    // don't include - wait for encoded file
                    log.Info("File: " + EncodedFileName + " does not exist");
                    return false;
                }

                if (isVideo)
                {
                    
                    EncodedThumbnal = unprocessedKey.Replace(OriginalPath, EncodedPath).Replace(".mp4", thumbnailSuffix+".jpg");
                    EncodedThumbnal = EncodedThumbnal.Replace("%20", " ");
                    log.Info("Checking existance of thumbnail File: " + EncodedThumbnal);
                    if (!_s3Service.CheckObjectExists(EncodedThumbnal))
                    {
                        log.Info("thumbnail File: " + EncodedThumbnal + " doe not exist.");
                        return false;
                    }
                }
                else
                {
                    EncodedThumbnal = FileName;
                }
                log.Info("Checking Size of File: " + EncodedFileName);
                FileSize = _s3Service.CheckFileSize(EncodedFileName);
            }
            catch
            {
                return false;
            }
            return true;
        }

    }
}
