using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Configuration;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Net;

namespace EncodingMonitor
{
    public partial class TestForm : Form
    {
        int _frequency;

        string _connectionString;
        string _awsAccessKey;
        string _awsSecretAccesskey;
        string _awsBucket;

        SqlConnection conn;
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
        public TestForm()
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

            conn = new SqlConnection(_connectionString);

            _frequency = int.Parse(ConfigurationManager.AppSettings["Frequency"].ToString());
            _s3Service = new AwsService(_awsAccessKey,_awsSecretAccesskey,_awsBucket);
        }

        private void button1_Click(object sender, EventArgs e)
        {
            DateTime start = DateTime.Now;
            List<RunInfo> configs = new List<RunInfo>();

            label1.Text = "working.....";

            try
            {
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }
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
                    }

                    foreach (Item it in items)
                    {
                        bool IsVideo = (it.UnprocessedName.Substring(it.UnprocessedName.LastIndexOf(".") + 1).ToLower() == "mp4");

                        // check for existance
                        string EncodedFileName = "";
                        string EncodedThumbnailName = "";
                        long FileSize = 0;
                        if (ProcessFile(it.UnprocessedName, IsVideo, out EncodedFileName, out EncodedThumbnailName, out FileSize, ri.OriginalPath, ri.EncodedPath))
                        {
                            // update record in database
                            UpdateRecord(ri.UpdateSPName, it.ItemId, EncodedFileName, EncodedThumbnailName, FileSize);
                        }
                    }
                }
            }

            catch (Exception ex)
            {
                label1.Text = "Exception: " + ex.Message;
                return;
            }
        
            TimeSpan span = DateTime.Now - start;
            int ms = (int)span.TotalMilliseconds;

            label1.Text = string.Format("Done in {0} milliseconds", ms);
        }

        private void UpdateRecord(string SPName, int ItemID, string EncodedFileName, string EncodedThumbnailName, long Size)
        {

                SqlCommand cmd = new SqlCommand(SPName, conn);
                cmd.CommandType = CommandType.StoredProcedure;

                cmd.Parameters.Add(new SqlParameter("@ItemId", ItemID));
                cmd.Parameters.Add(new SqlParameter("@EncodedFileName", EncodedFileName));
                cmd.Parameters.Add(new SqlParameter("@EncodedThumbnailName", EncodedThumbnailName));
                cmd.Parameters.Add(new SqlParameter("@FileSize", Size));

                cmd.ExecuteNonQuery();
            
        }

        private bool ProcessFile(string FileName, bool isVideo, out string EncodedFileName, out string EncodedThumbnal, out long FileSize, string OriginalPath, string EncodedPath)
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
                if (! isVideo)
                {
                    EncodedFileName = EncodedFileName.Replace(".png", ".jpg");
                }

                if (!_s3Service.CheckObjectExists(EncodedFileName))
                //if (!_s3Service.CheckObjectExists(FileName))
                    {
                    // don't include - wait for encoded file
                    return false;
                }

                if (isVideo)
                {
                    EncodedThumbnal = unprocessedKey.Replace(OriginalPath, EncodedPath).Replace(".mp4", "_thumb.jpg");
                    EncodedThumbnal = EncodedThumbnal.Replace("%20", " ");
                    if (!_s3Service.CheckObjectExists(EncodedThumbnal))
                    {
                        return false;
                    }
                }
                else
                {
                    EncodedThumbnal = FileName;
                }
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
