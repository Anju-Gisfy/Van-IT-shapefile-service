using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using System.IO;

namespace ShapefileService
{
    public class clsConnnection
    {

        public SqlConnection objCon;
        public SqlCommand objCom;
        public SqlDataReader objDr;
        public SqlDataAdapter objAdr;
        public DataSet objDs;
        public DataTable objDt;
        public StreamWriter logfile;
        public clsConnnection()
        {

            string connectionstring = "server=EC2AMAZ-G8A2PIA\\VANSYSTEM;database=VanIT;integrated security = true;Connection Timeout=0";
            objCon = new SqlConnection(connectionstring);
            try
            {
                //Random rn = new Random();
                //int order = rn.Next(100000, 99999999);
                //logfile = new StreamWriter("C:\\inetpub\\wwwroot\\SQL\\log\\ClsConnectionLogInNCTNM " + order.ToString() + DateTime.Now.Year + "" + DateTime.Now.Month + "" + DateTime.Now.Day + DateTime.Now.Hour + "_" + DateTime.Now.Minute + "_" + DateTime.Now.Second + "_" + DateTime.Now.Millisecond + "_" + DateTime.Now.Millisecond + ".txt", true);
            }
            catch (Exception ex)
            {
                //Random rn = new Random();
                //int order = rn.Next(10000000, 99999999);
                //logfile = new StreamWriter("C:\\inetpub\\wwwroot\\SQL\\log\\ClsConnectionLogInNCTNM " + order.ToString() + DateTime.Now.Year + "" + DateTime.Now.Month + "" + DateTime.Now.Day + DateTime.Now.Hour + "_" + DateTime.Now.Minute + "_" + DateTime.Now.Second + "_" + DateTime.Now.Millisecond + "_" + DateTime.Now.Millisecond + ".txt", true);
            }

        }

        public void fnOpenConnection()
        {

            if (objCon.State == ConnectionState.Closed)
            {
                objCon.Open();
            }
            //else
            //{
            //    objCon.Close();
            //    objCon.Open();
            //}
        }
        public void fnCloseConnection()
        {
            if (objCon.State == ConnectionState.Open)
            {
                objCon.Close();
            }
        }
        public SqlDataReader fnExecuteSelectStmtDr(string sqlstmt)
        {
            try
            {
                fnOpenConnection();
                objCom = new SqlCommand(sqlstmt, objCon);
                objDr = objCom.ExecuteReader();

                return objDr;
            }
            catch (Exception e) { return null; }
            finally { fnCloseConnection(); }
        }
        public DataTable fnExecuteSelectStmtDt(string sqlstmt)
        {
            try
            {
                fnOpenConnection();
                objAdr = new SqlDataAdapter(sqlstmt, objCon);
                objDt = new DataTable();
                objAdr.Fill(objDt);

                return objDt;
            }
            catch (Exception e) { return null; }
            finally { fnCloseConnection(); }
        }
        public DataSet fnExecuteSelectStmtDs(string sqlstmt)
        {
            try
            {
                fnOpenConnection();
                objAdr = new SqlDataAdapter(sqlstmt, objCon);
                objDs = new DataSet();
                objAdr.Fill(objDs);

                return objDs;
            }
            catch (Exception e) { return null; }
            finally { fnCloseConnection(); }

        }
        public DataTable fnExecuteProcedureSelect(string procedurename)
        {
            try
            {
                fnOpenConnection();
                objCom = new SqlCommand(procedurename, objCon);
                objCom.CommandType = CommandType.StoredProcedure;
                objAdr = new SqlDataAdapter(objCom);
                objDt = new DataTable();
                objAdr.Fill(objDt);

                return objDt;
            }
            catch (Exception e) { return null; }
            finally { fnCloseConnection(); }

        }
        //public  DataTable fnExecuteProcedureSelectWithCondtion(string procedurename, NameValueCollection ParamNamevalue)
        //{
        //    try
        //    {
        //        fnOpenConnection();
        //        objCom = new SqlCommand(procedurename, objCon);

        //        //objCom.CommandTimeout = -1;
        //        objCom.CommandType = CommandType.StoredProcedure;
        //        foreach (string key in ParamNamevalue.AllKeys)
        //        {
        //            string val = ParamNamevalue.Get(key);
        //            SqlParameter param = new SqlParameter(key, val);
        //            objCom.Parameters.Add(param);
        //        }
        //        objAdr = new SqlDataAdapter(objCom);
        //        objDt = new DataTable();
        //        objAdr.Fill(objDt);

        //        return objDt;
        //    }
        //    catch (Exception e) { return null; }
        //    finally { fnCloseConnection(); }

        //}
        public DataTable fnExecuteProcedureSelectWithCondtion(string procedurename, NameValueCollection ParamNamevalue)
        {
            try
            {
                fnOpenConnection();
                objCom = new SqlCommand(procedurename, objCon);

                //objCom.CommandTimeout = -1;
                objCom.CommandType = CommandType.StoredProcedure;
                objCom.CommandTimeout = 0;
                foreach (string key in ParamNamevalue.AllKeys)
                {
                    string val = ParamNamevalue.Get(key);
                    SqlParameter param = new SqlParameter(key, val);
                    objCom.Parameters.Add(param);
                    //logfile.WriteLine("key=>" + key);
                    //logfile.Flush();
                    //logfile.WriteLine("val=>" + val);
                    //logfile.Flush();
                    //logfile.WriteLine("Procedure name " + procedurename);
                    //logfile.Flush();
                }
                objAdr = new SqlDataAdapter(objCom);
                objDt = new DataTable();
                objAdr.Fill(objDt);

                return objDt;
            }
            catch (Exception e)
            {
                //    logfile.WriteLine("Error occured in fnExecuteProcedureSelectWithCondtion function definition" + DateTime.Now.Year + "-" + DateTime.Now.Month + "-" + DateTime.Now.Day + " " + DateTime.Now.Hour + ":" + DateTime.Now.Minute + ":" + DateTime.Now.Second + ":" + DateTime.Now.Millisecond);
                //    logfile.Flush();

                //    logfile.WriteLine("Message=>" + e.Message);
                //    logfile.Flush();
                //    logfile.WriteLine("StackTrace=>" + e.StackTrace);
                //    logfile.Flush();
                //    logfile.WriteLine("InnerException=>" + e.InnerException);
                //    logfile.Flush();

                return null;
            }
            finally { fnCloseConnection(); }

        }

        public DataTable fnExecuteProcedureSelectSpecificDt(string procedurename, NameValueCollection ParamNamevalue)
        {
            try
            {
                fnOpenConnection();
                objCom = new SqlCommand(procedurename, objCon);
                objCom.CommandType = CommandType.StoredProcedure;
                foreach (string key in ParamNamevalue.AllKeys)
                {
                    string val = ParamNamevalue.Get(key);
                    SqlParameter param = new SqlParameter(key, val);
                    objCom.Parameters.Add(param);
                }
                objAdr = new SqlDataAdapter(objCom);


                objDt = new DataTable();
                objAdr.Fill(objDt);

                return objDt;
            }
            catch (Exception e) { return null; }
            finally { fnCloseConnection(); }

        }

        public string fnExecuteProcedureReturnSingle(string procedurename, NameValueCollection ParamNamevalue)
        {
            try
            {
                fnOpenConnection();
                objCom = new SqlCommand(procedurename, objCon);
                objCom.CommandType = CommandType.StoredProcedure;
                objCom.CommandTimeout = 0;
                foreach (string key in ParamNamevalue.AllKeys)
                {
                    string val = ParamNamevalue.Get(key);
                    SqlParameter param = new SqlParameter(key, val);
                    objCom.Parameters.Add(param);
                }
                string rec = objCom.ExecuteScalar().ToString();

                return rec;
            }
            catch (Exception e) { return null; }
            finally { fnCloseConnection(); }

        }

        public string fnReplaceSpecialCharComments(string str)
        {

            str = str.Replace("!", "EMark").Replace("\"", "QMark").Replace("#", "CHatch").Replace("$", "DSign").Replace("%", "PSign").Replace("&", "Ampersand").Replace("`", "CSQuote").Replace("(", "OParentheses").Replace(")", "CParentheses").Replace("*", "Asterisk").Replace("+", "Plus").Replace("-", "Hyphen").Replace("/", "fSlash").Replace(":", "Colon").Replace(";", "Semicolon").Replace("<", "LTSign").Replace("=", "ESign").Replace(">", "GTSign").Replace("?", "QMark").Replace("@", "AtSign").Replace("[", "OSBracket").Replace("]", "ClSBracket").Replace("^", "Circflex").Replace("_", "Uscore").Replace("'", "OSQuote").Replace("{", "OCBrace").Replace("|", "VLine").Replace("}", "CCBrace").Replace("~", "Tilde").Trim().Replace("\r\n", "F9").Replace("</br>", "F9");
            return str;
        }
        public string fnReadSpecialCharComments(string str)
        {
            str = str.Replace("EMark", "!").Replace("QMark", "\"").Replace("CHatch", "#").Replace("DSign", "$").Replace("PSign", "%").Replace("Ampersand", "&").Replace("CSQuote", "`").Replace("OParentheses", "(").Replace("CParentheses", ")").Replace("Asterisk", "*").Replace("Plus", "+").Replace("Hyphen", "-").Replace("fSlash", "/").Replace("Colon", ":").Replace("Semicolon", ";").Replace("LTSign", "<").Replace("ESign", "=").Replace("GTSign", ">").Replace("QMark", "?").Replace("AtSign", "@").Replace("OSBracket", "[").Replace("ClSBracket", "]").Replace("Circflex", "^").Replace("Uscore", "_").Replace("OSQuote", "'").Replace("OCBrace", "{").Replace("VLine", "|").Replace("CCBrace", "}").Replace("Tilde", "~").Trim().Replace("F9", "</br>");
            return str;
        }
        public DataSet fnExecuteProcedureSelectWithCondtionDataset(string procedurename, NameValueCollection ParamNamevalue)
        {
            try
            {
                fnOpenConnection();
                objCom = new SqlCommand(procedurename, objCon);

                //objCom.CommandTimeout = -1;
                objCom.CommandType = CommandType.StoredProcedure;
                objCom.CommandTimeout = 0;
                foreach (string key in ParamNamevalue.AllKeys)
                {
                    string val = ParamNamevalue.Get(key);
                    SqlParameter param = new SqlParameter(key, val);
                    objCom.Parameters.Add(param);
                }
                objAdr = new SqlDataAdapter(objCom);
                DataSet objDts = new DataSet();
                objAdr.Fill(objDts);
                return objDts;
            }
            catch (Exception e)
            {
                return null;
            }
            finally { fnCloseConnection(); }

        }
    }
}