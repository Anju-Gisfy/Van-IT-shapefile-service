using System;
using System.Diagnostics;
using System.ServiceProcess;
using System.Threading;
using System.IO;
using System.Text;
using Microsoft.VisualBasic;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Specialized;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;
using System.Reflection.Emit;
using System.Xml.Linq;

namespace ShapefileService
{
    public partial class Service1 : ServiceBase
    {


        bool serviceStarted; //checks service status 
        Thread faxWorkerThread;
        //  Thread mailingThread;
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {

            WriteToFile("Service is started at " + DateTime.Now);

            ThreadStart start = new ThreadStart(getstarted); // FaxWorker is where the work gets done
            faxWorkerThread = new Thread(start);
            serviceStarted = true;

            // start threads
            faxWorkerThread.Start();

        }

        protected override void OnStop()
        {
            WriteToFile("Service is stop at " + DateTime.Now);
            test();
            base.OnStop();
        }
        public void getstarted()
        {
            while (true)
            {
                test();

            }
        }

        public void test()
        {


            int count = 0;
            string tablename = "newlayer";
            // < add name = "Root" connectionString = "D:\Division-shapefilezip\Divisions\extracted\" />
            string folderPath = @"C:\ShapefilesService-Test\";
            string[] shapefiles = Directory.GetFiles(folderPath, "*.shp");

            // string destinationFolderPath = @"D:\Division-shapefilezip\Divisions\ShapeFileBackup\";
            string destinationFolderPath = @"C:\ShapeFileBackup\";
            string conn = "server=EC2AMAZ-G8A2PIA\\VANSYSTEM;database=VanIT;integrated security = true;Connection Timeout=0";
            DataTable dt = new DataTable();
            foreach (string shapefile in shapefiles)
            {
                WriteToFile("Shapefile found " + shapefile + " at " + DateTime.Now);
                string shapefileName = Path.GetFileNameWithoutExtension(shapefile);


                try
                {

                    NameValueCollection nvc = new NameValueCollection();
                    nvc.Clear();
                    nvc.Add("@StatementType", "Select");
                    nvc.Add("@filename", shapefileName);

                    //nvc.Add("@UserName", UserName);
                    dt = new clsConnnection().fnExecuteProcedureSelectWithCondtion("[VanIT].[dbo].[sp_shapefile_queue]", nvc);


                    using (dt)
                    {
                        //  sda.Fill(dt);
                        if (dt.Rows.Count > 0)
                        {
                            string command = "ogr2ogr -f MSSQLSpatial \"MSSQL:server=EC2AMAZ-G8A2PIA\\VANSYSTEM;driver={ODBC Driver 17 for SQL Server};database=VanIT;uid=sa;pwd=pass@123\" C:\\ShapefilesService-Test\\" + shapefileName + ".shp -a_srs EPSG:4326 -nln " + tablename + " --config MSSQLSPATIAL_USE_BCP FALSE";


                            Process process = new Process();
                            process.StartInfo.FileName = @"C:\OSGeo4W\OSGeo4W.bat";
                            process.StartInfo.WorkingDirectory = @"C:\OSGeo4W"; // set the working directory to the OSGeo4W64 folder
                            process.StartInfo.Arguments = command;

                            //temp table creation
                            try
                            {
                                // Start the process
                                process.Start();

                                process.WaitForExit();


                                WriteToFile("command running " + DateTime.Now);
                            }
                            catch (Exception ex)
                            {
                                WriteToFile("Error: " + ex + " at " + DateTime.Now);
                            }


                            WriteToFile("command run successfully at " + DateTime.Now);

                            //update Is-delete to true in queue table
                            try
                            {

                                clsConnnection clscon = new clsConnnection();
                                clscon.objCom = new SqlCommand("[VanIT].[dbo].[sp_shapefile_queue]", clscon.objCon);


                                //SqlCommand cmdn = new SqlCommand("sp_shapefile_queue", con);
                                clscon.objCom.CommandType = CommandType.StoredProcedure;
                                clscon.objCom.Parameters.AddWithValue("@StatementType", "Update");
                                clscon.objCom.Parameters.AddWithValue("@filename", shapefileName);
                                clscon.objCom.Parameters.AddWithValue("@shapefileid", dt.Rows[0]["id"]);
                                clscon.fnOpenConnection();
                                int i = clscon.objCom.ExecuteNonQuery();
                                clscon.fnCloseConnection();
                                if (i > 0)
                                {
                                    WriteToFile("Updated queue table as deleted at " + DateTime.Now);
                                }
                            }
                            catch (Exception ex) { }
                            try
                            {
                                //to get division name
                                nvc.Clear();
                                nvc.Add("@StatementType", dt.Rows[0]["boundary"].ToString());
                                nvc.Add("@operation", "select");
                                nvc.Add("@tablename", tablename);

                                //nvc.Add("@UserName", UserName);
                                DataTable dtn = new clsConnnection().fnExecuteProcedureSelectWithCondtion("[VanIT].[dbo].[sp_shapefile_import]", nvc);

                                DataTable dtss = new DataTable();
                                using (dtn)
                                {
                                    string viewname = "vw_" + dt.Rows[0]["boundary"].ToString().ToLower() + "_" + dtn.Rows[0]["namedivision"].ToString().ToLower();
                                    if (dtn.Rows.Count > 0)
                                    {
                                        for (int x = 0; x < dtn.Rows.Count; x++)
                                        {
                                            try
                                            {
                                                //checking the division data already inserted or not

                                                nvc.Clear();
                                                nvc.Add("@StatementType", dt.Rows[0]["boundary"].ToString());
                                                nvc.Add("@operation", "selectdata");
                                                nvc.Add("@divisionname", dtn.Rows[x]["DivisionName"].ToString());

                                                //nvc.Add("@UserName", UserName);
                                                dtss = new clsConnnection().fnExecuteProcedureSelectWithCondtion("[VanIT].[dbo].[sp_shapefile_import]", nvc);
                                            }
                                            catch (Exception ex) { }

                                            try
                                            {

                                                //update orginal data if already there
                                                if ((dt.Rows[0]["is_delete"].ToString() == "true") || (dtss.Rows.Count > 0))
                                                {
                                                    try
                                                    {
                                                        clsConnnection clscon = new clsConnnection();
                                                        clscon.objCom = new SqlCommand("[VanIT].[dbo].[sp_shapefile_import]", clscon.objCon);


                                                        //SqlCommand cmdn = new SqlCommand("sp_shapefile_queue", con);
                                                        clscon.objCom.CommandType = CommandType.StoredProcedure;
                                                        clscon.objCom.Parameters.AddWithValue("@StatementType", dt.Rows[0]["boundary"]);
                                                        clscon.objCom.Parameters.AddWithValue("@operation", "update");
                                                        clscon.objCom.Parameters.AddWithValue("@divisionname", dtn.Rows[x]["DivisionName"].ToString());

                                                        clscon.fnOpenConnection();
                                                        int k = clscon.objCom.ExecuteNonQuery();
                                                        clscon.fnCloseConnection();
                                                        if (k > 0)
                                                        {
                                                            count = count + 1;
                                                        }
                                                            WriteToFile("original table updated at " + DateTime.Now);


                                                    }
                                                    catch (Exception ex) { }
                                                   
                                                }
                                                else
                                                {
                                                    //moving data from temp table to original table
                                                    try
                                                    {


                                                        clsConnnection clscon = new clsConnnection();
                                                        clscon.objCom = new SqlCommand("[VanIT].[dbo].[sp_shapefile_import]", clscon.objCon);


                                                        //SqlCommand cmdn = new SqlCommand("sp_shapefile_queue", con);
                                                        clscon.objCom.CommandType = CommandType.StoredProcedure;
                                                        clscon.objCom.Parameters.AddWithValue("@StatementType", dt.Rows[0]["boundary"]);
                                                        clscon.objCom.Parameters.AddWithValue("@operation", "insert");
                                                        clscon.objCom.Parameters.AddWithValue("@tablename", tablename);
                                                        clscon.objCom.Parameters.AddWithValue("@divisionname", dtn.Rows[x]["DivisionName"].ToString());

                                                        clscon.fnOpenConnection();
                                                        int i = clscon.objCom.ExecuteNonQuery();
                                                        clscon.fnCloseConnection();
                                                        if (i > 0)
                                                        {
                                                            count = count + 1;
                                                            WriteToFile("selected and inserted data at " + DateTime.Now);
                                                        }


                                                    }
                                                    catch (Exception ex) { }

                                                   
                                                }



                                            }
                                            catch (Exception ex) { }
                                        }

                                       
                                        //delete views if already there
                                        try
                                        {
                                            clsConnnection clscon = new clsConnnection();
                                            clscon.objCom = new SqlCommand("[VanIT].[dbo].[sp_shapefile_import]", clscon.objCon);


                                            //SqlCommand cmdn = new SqlCommand("sp_shapefile_queue", con);
                                            clscon.objCom.CommandType = CommandType.StoredProcedure;
                                            clscon.objCom.Parameters.AddWithValue("@StatementType", "deleteview");
                                            clscon.objCom.Parameters.AddWithValue("@tablename", viewname);

                                            clscon.fnOpenConnection();
                                            int k = clscon.objCom.ExecuteNonQuery();
                                            clscon.fnCloseConnection();

                                            WriteToFile("views deleted at " + DateTime.Now);


                                        }
                                        catch (Exception ex) { }
                                        //creating new view
                                        try
                                        {
                                               
                                                string divisonname = "'" + dtn.Rows[0]["namedivision"].ToString() + "'";
                                                clsConnnection clscon = new clsConnnection();
                                                clscon.objCom = new SqlCommand("[VanIT].[dbo].[sp_shapefile_import]", clscon.objCon);


                                                //SqlCommand cmdn = new SqlCommand("sp_shapefile_queue", con);
                                                clscon.objCom.CommandType = CommandType.StoredProcedure;
                                                clscon.objCom.Parameters.AddWithValue("@StatementType", "createview");
                                                clscon.objCom.Parameters.AddWithValue("@operation", dt.Rows[0]["boundary"]);
                                                clscon.objCom.Parameters.AddWithValue("@tablename", viewname);
                                                clscon.objCom.Parameters.AddWithValue("@divisionname", divisonname);

                                                clscon.fnOpenConnection();
                                                int k = clscon.objCom.ExecuteNonQuery();
                                                clscon.fnCloseConnection();

                                                WriteToFile("views created at " + DateTime.Now);


                                            }
                                            catch (Exception ex) { }
                                        
                                    }



                                    //create layer
                                    try
                                    {
                                        string commandn = "curl -v -u admin:geoserver -XPOST -H \"Content-type: text/xml\" -d \"<featureType><name>" + viewname + "</name></featureType>\" http://3.7.34.230:8080/geoserver/rest/workspaces/cite/datastores/Van%20IT%20Shapefiles/featuretypes";

                                        ProcessStartInfo startInfon = new ProcessStartInfo();
                                        startInfon.FileName = "cmd.exe";
                                        startInfon.Arguments = "/c " + commandn;
                                        startInfon.WindowStyle = ProcessWindowStyle.Hidden;
                                        startInfon.RedirectStandardOutput = true;
                                        startInfon.UseShellExecute = false;

                                        Process processn = new Process();
                                        processn.StartInfo = startInfon;
                                        processn.Start();
                                        processn.WaitForExit();
                                        string output = processn.StandardOutput.ReadToEnd();

                                        WriteToFile("layer created at " + DateTime.Now);


                                    }
                                    catch (Exception ex)
                                    {
                                        WriteToFile("Error layer creation at " + DateTime.Now);
                                    }

                                    //layer styling
                                    try
                                    {
                                        string stylename = "";
                                        string boundary = dt.Rows[0]["boundary"].ToString();
                                        if (boundary == "Division")
                                        {
                                            stylename = "division_style1";
                                        }
                                        else if (boundary == "Range")
                                        {
                                            stylename = "range_style";
                                        }
                                        else if (boundary == "Block")
                                        {
                                            stylename = "block_style";
                                        }
                                        //curl -v -u admin:geoserver -XPUT -H "Content-type: text/xml" -d "<layer><defaultStyle><name>division_style1</name></defaultStyle></layer>" "http://localhost:8080/geoserver/rest/layers/cite:vw_division_tvm"

                                        string commandn = "curl -v -u admin:geoserver -XPUT -H \"Content-type: text/xml\" -d \"<layer><defaultStyle><name>"+stylename+"</name></defaultStyle></layer>\" \"http://localhost:8080/geoserver/rest/layers/cite:" + viewname + "\"";

                                        ProcessStartInfo startInfon = new ProcessStartInfo();
                                        startInfon.FileName = "cmd.exe";
                                        startInfon.Arguments = "/c " + commandn;
                                        startInfon.WindowStyle = ProcessWindowStyle.Hidden;
                                        startInfon.RedirectStandardOutput = true;
                                        startInfon.UseShellExecute = false;
                                        Process processn = new Process();
                                        processn.StartInfo = startInfon;
                                        processn.Start();
                                        processn.WaitForExit();
                                        string output = processn.StandardOutput.ReadToEnd();
                                        WriteToFile("style implemented at " + DateTime.Now);


                                    }
                                    catch (Exception ex)
                                    {
                                        WriteToFile("Error layer styling at " + DateTime.Now);
                                    }
                                }
                            }
                            catch (Exception ex) { }

                            //temp table deleted
                            if (count > 0)
                            {
                                try
                                {
                                    clsConnnection clscon = new clsConnnection();
                                    clscon.objCom = new SqlCommand("[VanIT].[dbo].[sp_shapefile_import]", clscon.objCon);


                                    //SqlCommand cmdn = new SqlCommand("sp_shapefile_queue", con);
                                    clscon.objCom.CommandType = CommandType.StoredProcedure;
                                    clscon.objCom.Parameters.AddWithValue("@StatementType", "Delete");
                                    clscon.objCom.Parameters.AddWithValue("@tablename", tablename);

                                    clscon.fnOpenConnection();
                                    int k = clscon.objCom.ExecuteNonQuery();
                                    clscon.fnCloseConnection();

                                    WriteToFile("temp table deleted at " + DateTime.Now);


                                }
                                catch (Exception ex) { }
                            }
                        }

                    }


                }
                catch (Exception ex) { }





            }


            foreach (string sourceFilePath in Directory.GetFiles(folderPath))
            {
                string fileName = Path.GetFileName(sourceFilePath);
                string destinationFilePath = Path.Combine(destinationFolderPath, fileName);
                try
                {
                    if (File.Exists(destinationFilePath))
                    {
                        File.Delete(destinationFilePath);
                        File.Move(sourceFilePath, destinationFilePath);

                        WriteToFile("file successfully moved " + fileName + " at " + DateTime.Now);
                    }
                    else
                    {
                        File.Move(sourceFilePath, destinationFilePath);

                        WriteToFile("file successfully moved " + fileName + " at " + DateTime.Now);
                    }
                }
                catch (Exception ex)
                {
                    WriteToFile(ex + " at " + DateTime.Now);
                }

            }

        }
        public void WriteToFile(string Message)
        {

            string path = AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\ServiceLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
            if (!File.Exists(filepath))
            {
                // Create a file to write to.   
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }


    }
}
