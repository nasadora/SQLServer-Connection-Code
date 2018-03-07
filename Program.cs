using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Xml;
using System.Text.RegularExpressions;


namespace SQLServer_data_source_DIFS
{
    class Program
    {
        // will only use windows authentication, the db is not setup to log into ohter ways

        static void Main(string[] args)
        {
        
            try
            {
                // read xml file
                XmlDocument document = new XmlDocument();
                document.Load(Path.Combine(Environment.CurrentDirectory, "Config.xml"));

                XmlNode node = document.SelectSingleNode("/Config");

                string sqlstringquery = node["SqlString"].InnerText;
                string[] sqlforcountarray = Regex.Split(sqlstringquery, "FROM ");
                string sqlforcount = sqlforcountarray[1];
                
                //Console.WriteLine(node["SqlString"].InnerText);
                string filepath = node["FilePath"].InnerText;

                string filepathloc = node["FilePath"].InnerText + node["FileName"].InnerText;
                //Console.WriteLine("File Path: {0}", node["FilePath"].InnerText);
                
                string filestringname = node["FileName"].InnerText;
                //Console.WriteLine("File Name: {0}", node["FileName"].InnerText);

                string logName = node["LogFileName"].InnerText;
                //Console.WriteLine("LogFileName: {0}", node["LogFileName"].InnerText);

                string dbsrvr = node["DatabaseServer"].InnerText;
                //Console.WriteLine("Database Server: {0}", node["DatabaseServer"].InnerText);

                string dbname = node["Database"].InnerText;
                //Console.WriteLine("Database: {0}", node["Database"].InnerText);
                
                string IntSec = node["IntegratedSecurity"].InnerText;
                //Console.WriteLine("Integrated Security: {0}", node["IntegratedSecurity"].InnerText);

                string combinedSqlString = "server=" + dbsrvr + ";database=" + dbname + ";Integrated Security=" + IntSec;
                string connectionString = combinedSqlString;

                Program a = new Program();
                int newProdID = a.queryCount(sqlforcount, "test", combinedSqlString);
                string test4 = newProdID.ToString();

                try // try to connect and create the csv file
                {
                    DateTime datetime = DateTime.Now;
                    string MDYformat = "dd/MMM/yyyy hh:mm:ss";
                    string justDate1 = datetime.ToString(MDYformat);
                    string filePathLog = filepath + logName + ".log";
                    
                    //TODO: writing log 1. SQLServer-data-source-DIFS version: 1.1
                    File.WriteAllText(filePathLog, "SQLServer-data-source-DIFS version: 1.1\n");
              
                    //TODO: writing log 2. Process Started & Date
                    string[] proStart = { "\nProcess Started : " + justDate1 + "\n" };
                    //Console.WriteLine("process started : " + justDate1);
                    File.AppendAllLines(filepath + logName + ".log", proStart);
               
                    using (StreamWriter theWriter = new StreamWriter(filepathloc))
                    {
                        datetime = DateTime.Now;
                        string justDate2 = datetime.ToString(MDYformat);
                        //Console.WriteLine("File Name: {0}", node["FileName"].InnerText + " created : " + justDate2);
                  
                        string[] csv_crtd = { filestringname + " : " + justDate2};
                        File.AppendAllLines(filepath + logName + ".log", csv_crtd);

                        try // try the SQL connection
                        {
                            using (SqlConnection connection = new SqlConnection(connectionString))
                            {
                                connection.Open();
                                
                                datetime = DateTime.Now;
                                string justDate3 = datetime.ToString(MDYformat);
                                //Console.WriteLine("Connected to sqlserver : " + justDate3);

                                //TODO: writing log 3. Connected to sql server
                                string [] connToSQL = { "\n" + "Connected to sqlserver" + " : " + justDate3 };                                
                                File.AppendAllLines(filepath + logName + ".log" , connToSQL);

                                string cmdQ = sqlstringquery;

                                // Do work here; connection closed on following line.
                                using (SqlCommand myCommand = new SqlCommand(cmdQ, connection))
                                {
                                    using (SqlDataAdapter sda = new SqlDataAdapter())
                                    {
                                        datetime = DateTime.Now;
                                        string justDate4 = datetime.ToString(MDYformat);                                       
                                        //Console.WriteLine("Database: {0}", node["Database"].InnerText + " opened:" + justDate4);
                                        //TODO: writing log 4. db & table opened
                                        string[] dbopened = { "\n" + sqlforcount + " : " + justDate4 };
                                        File.AppendAllLines(filepath + logName + ".log", dbopened);
                                        sda.SelectCommand = myCommand;

                                        using (DataTable dt = new DataTable())
                                        {

                                            sda.Fill(dt);

                                            //Build the CSV file data as a Comma separated string.
                                            string csv = string.Empty;

                                            foreach (DataColumn column in dt.Columns)
                                            {

                                                //Add the Header row for CSV file.

                                                csv += column.ColumnName + ',';


                                            }
                                            theWriter.WriteLine(csv);
                                            csv = string.Empty;
                                            foreach (DataRow row in dt.Rows)
                                            {
                                                foreach (DataColumn column in dt.Columns)
                                                {
                                                    //Add the Data rows.

                                                    csv += row[column.ColumnName].ToString().Replace(",", ";") + ',';

                                                }
                                                //Add new line.
                                                theWriter.WriteLine(csv);
                                                csv = string.Empty;
                                            }
                                            //Download the CSV file.
                                        }

                                    }
                                }
                            }

                        }// try the SQL connection
                        catch
                        {
                            datetime = DateTime.Now;
                            string justDate5 = datetime.ToString(MDYformat);
                            //TODO: writing log if sql connection error
                            string[] msg = { "SQL connection Error : " + justDate5 };
                            File.AppendAllLines(filepath + logName + ".log", msg);
                      
                        }
                    }
                    //TODO: writing 5. log Records exported
                    datetime = DateTime.Now;
                    string justDate6 = datetime.ToString(MDYformat);
                    string[] rec_exp = { "\nRecords Exported" + " : " + test4 };
                    File.AppendAllLines(filepath + logName + ".log", rec_exp);
                   
                    //TODO: writing 6. log Process Finished
                    string justDate7 = datetime.ToString(MDYformat);
                    string[] proFinished = { "\nProcess finished : " + justDate7 };
                    File.AppendAllLines(filepath + logName + ".log", proFinished);
                    //Console.WriteLine("Process finished : " + justDate7);
                    string text;

                    using (StreamReader r = new StreamReader(filepathloc))
                    {
                        text = r.ReadToEnd();
                    }

                    Regex RE = new Regex("\\n", RegexOptions.Multiline);
                    MatchCollection theMatches = RE.Matches(text);
                    int test = theMatches.Count - 1;
                    string test2 = test.ToString();
                  
                }// try to connect and create the csv file
                catch
                {
                    DateTime datetime = DateTime.Now;
                    string MDYformat = "dd/MMM/yyyy hh:mm:ss";
                    string justDate7 = datetime.ToString(MDYformat);
                    //Console.WriteLine("csv file failed to create: " + justDate7);
                    string[] lines = { "csv file failed to create: " + justDate7 };
                    File.WriteAllLines(filepath + logName + "-" + DateTime.Now.ToString("yyyy-dd-M-HH-mm-ss") + ".log", lines);

                }

            }
            catch (FileNotFoundException ex)
            {
                //Log the details to the DB
                Console.WriteLine("Please check if the file {0} exists", ex.FileName);
              
            }
            catch (Exception ex)
            {
                //Log the details to the DB
                Console.WriteLine(ex.Message);
             
            }
        
        }

        public int queryCount(string sqlforcount, string newName, string connString = null)
        {
            Int32 newProdID = 0;
            string sql =
                "SELECT count(*) FROM " + sqlforcount;

            using (SqlConnection conn = new SqlConnection(connString))
            {
                SqlCommand cmd = new SqlCommand(sql, conn);
                cmd.Parameters.Add("@Name", SqlDbType.VarChar);
                cmd.Parameters["@name"].Value = newName;
                try
                {
                    conn.Open();
                    newProdID = (Int32)cmd.ExecuteScalar();

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            return (int)newProdID;
        }

    }
}
