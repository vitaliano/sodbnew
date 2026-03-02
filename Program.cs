using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Web;
using System.Configuration;
using System.Collections.Specialized;
using System.Xml;
using System.Diagnostics;
using System.Threading;
using System.Timers;

namespace SODB_NEW
{
    public enum server_status { off, no_data, with_data, no_auth };
    class Program
    {
        private static System.Timers.Timer aTimer;
        //TODO 
        // por no agendador para rodar a cada 10 minutos
        // interromper se estiver rodando ha mais de 3 horas
        private static void SetTimer()
        {
            // Create a timer with a two second interval.
            aTimer = new System.Timers.Timer(10000);
            // Hook up the Elapsed event for the timer. 
            aTimer.Elapsed += OnTimedEvent;
            aTimer.AutoReset = true;
            aTimer.Enabled = true;
        }

        private static void OnTimedEvent(Object source, ElapsedEventArgs e)
        {
            Console.WriteLine("The Elapsed event was raised at {0:HH:mm:ss.fff}",
                              e.SignalTime);
        }
        
        static string getXMLFileString(string pathXMLFile)
        {
            string myResponse = "";
            try
            {
                XmlTextReader reader = new XmlTextReader(pathXMLFile);
                while (reader.Read())
                {
                    if (reader.NodeType == XmlNodeType.Element &&
                        reader.Name == "configuration")
                    {
                        myResponse = reader.ReadOuterXml();
                    }
                }
                reader.Close();
            }
            catch
            {
                myResponse = "";
            }
            return myResponse;
        }
        static string getDateToTransmit(SqlConnection conn ,string lastTransmission)
        {
            // obtem a próxima data a transmitir
            // é a primeira data maior que dateStr
            string strSQL = @"select top 1 Convert(nvarchar(13),[created_at],121) as dateStr 
                            from totals_shop where created_at > CONVERT(DATETIME,'" + lastTransmission + ":59:59.999',121)";
            try
            {
                DataView dv = getSelection(conn, strSQL);
                if (dv != null)
                    if (dv.Count == 1)
                        return dv[0]["datestr"].ToString();
                    else
                        return "";
                else
                    return "";
            }
            catch (Exception ex)
            {
                logMessage("Exception in getDateToTransmit : " + ex.Message);
                return "";
            }
        }
        static DataView getSumsForTheHourOneLinePerCamera(SqlConnection conn,string dateToTransmit)
        {
            string dt1 = dateToTransmit + ":00:00";
            string dt2 = dateToTransmit + ":59:59.999";

            string strSQL =
@"SELECT computer_name, channel, shop_name, shop_door,
SUM(total_inside) AS ins,SUM(total_outside) AS outs FROM
(SELECT computer_name, channel, shop_name, shop_door, total_inside,total_outside
FROM totals_shop WHERE  created_at BETWEEN CONVERT(DATETIME,'@dt1',121) AND CONVERT(DATETIME,'@dt2',121) ) A
GROUP BY computer_name,channel, shop_name,shop_door";
            strSQL=strSQL.Replace("@dt1", dt1);
            strSQL=strSQL.Replace("@dt2", dt2);
            DataView dv = getSelection(conn, strSQL);
            return dv;
        }
        static DataView getSelection(SqlConnection conn, string query)
        {
            DataView dv = new DataView();
            DataSet ds = new DataSet();
            try
            {
                SqlDataAdapter userAdapter = new SqlDataAdapter(query, conn);
                userAdapter.Fill(ds, "all_users");
                dv = ds.Tables[0].DefaultView;

                if (dv.Count == 0)
                    Console.WriteLine("No Selection");
                return dv;
            }
            catch (Exception ex)
            {
                logMessage("Exception in getSelection : " + ex.Message);
                return null;
            }
        }
        private static string getXMLForTheHourOneLinePerCamera(string created_at,DataView dv, XmlDocument docConfig)
        {
            string thisTransmission = "";
            string door = "";
            string entrance = "";
            XmlDocument docDest = new XmlDocument();
            docDest.LoadXml("<catalog><flow_list></flow_list></catalog>");

            XmlNode fatherDest = docDest.GetElementsByTagName("flow_list")[0];
            int ch = 0;
            foreach (DataRowView rowX in dv)
            {
                string cn = rowX["computer_name"].ToString();
                string shn = rowX["shop_name"].ToString();
                string shd = rowX["shop_door"].ToString();
                int.TryParse(rowX["channel"].ToString(), out ch);
                mapDoor(docConfig,cn,shn,shd,ch,ref door,ref entrance);
                if (door == "")
                    continue;
                XmlNode nDest = docDest.CreateNode(XmlNodeType.Element, "this_transmission", "");

                XmlNode attr1 = docDest.CreateNode(XmlNodeType.Attribute, "door", "");
                attr1.Value = door;
                nDest.Attributes.SetNamedItem(attr1);

                XmlNode attr2 = docDest.CreateNode(XmlNodeType.Attribute, "entrance", "");
                attr2.Value = entrance;
                nDest.Attributes.SetNamedItem(attr2);

                XmlNode attr3 = docDest.CreateNode(XmlNodeType.Attribute, "created_at", "");
                attr3.Value = created_at+":59:59";
                nDest.Attributes.SetNamedItem(attr3);

                XmlNode attr4 = docDest.CreateNode(XmlNodeType.Attribute, "total_inside", "");
                attr4.Value = rowX["ins"].ToString().Trim();
                nDest.Attributes.SetNamedItem(attr4);

                XmlNode attr5 = docDest.CreateNode(XmlNodeType.Attribute, "total_outside", "");
                attr5.Value = rowX["outs"].ToString().Trim();
                nDest.Attributes.SetNamedItem(attr5);

                fatherDest.AppendChild(nDest);
            }
            thisTransmission = docDest.OuterXml;
            return thisTransmission;
        }
        static void mapDoor(XmlDocument doc,string cn,string shn,string shd, int ch,
                            ref string door,ref string entrance)
        {
            string chs = ch.ToString().Trim();
            string attrList = "old_shop_name,old_computer_name,old_door_name,old_channel";
            string searchedStr = shn + "," + cn + "," + shd + "," + chs;
            int ret = findATagNameWithAttr(doc, "door", attrList, searchedStr);
            if (ret == -1)
                door = "";
            else
            {
                door = doc.GetElementsByTagName("door")[ret].Attributes["door"].Value;
                entrance = doc.GetElementsByTagName("door")[ret].Attributes["entrance"].Value;
            }
        }
        static int findATagNameWithAttr(XmlDocument doc, string tagName,string attrs_names,string attrs_values)
        {
            string[] attr_name = attrs_names.Split(',');
            string[] attr_value = attrs_values.Split(',');
            int n = attr_name.Count();
            if (n != attr_value.Count())
                return -1;
            int ret = -1;
            foreach (XmlNode nodeTest in doc.GetElementsByTagName(tagName))
            {
                ret++;
                XmlAttributeCollection attrs = nodeTest.Attributes;
                int i = 0;
                for (i = 0; i < n; i++)
                {
                    if (attrs[attr_name[i]].Value != attr_value[i])
                        break;
                }
                if (i == n)
                    return ret; // o index do no 
                else
                    continue;
            }
            return -1;
        }
        public static int sendTransmission(string uri, string username,
                           string pw, string client,
                           string location, string transmission)
        {
            string rspStr = "";
            System.Net.WebRequest req = null;
            System.Net.WebResponse rsp = null;

            try
            {
                string qS = "?report_type=cameras_count_sent" +
                            "&user_name=" + username +
                            "&pw=" + pw +
                            "&client=" + client +
                            "&location=" + location +
                            "&app_type=1";
                uri = uri + qS;
                req = System.Net.WebRequest.Create(uri);
                req.Method = "POST";
                req.ContentType = "text/xml";

                System.IO.StreamWriter writer =
                        new System.IO.StreamWriter(req.GetRequestStream());
                writer.WriteLine(transmission);
                writer.Close();
                rsp = req.GetResponse();
                System.IO.StreamReader reader = new System.IO.StreamReader(rsp.GetResponseStream());
                rspStr = reader.ReadToEnd();// OK
                if (req != null)
                    req.GetRequestStream().Close();
                if (rsp != null)
                    rsp.GetResponseStream().Close();
                if (rspStr.Substring(0, 2) == "OK")
                    return (int)server_status.with_data;
                else
                    return (int)server_status.off;
            }
            catch
            {
                return (int)server_status.off;
            }
        }
        public static void logTransmissions(string transmission)
        {
            string serverPathTxt = AppDomain.CurrentDomain.BaseDirectory + "App_Data\\Transmissions.csv";
            string hourBegin = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(transmission);
            string line = "";
            foreach (XmlNode thisCam in doc.GetElementsByTagName("flow_list")[0])
            {
                XmlAttributeCollection attrs = thisCam.Attributes;
                line =
                    hourBegin + ";" +
                        attrs["created_at"].Value.Trim() + ";" +
                        attrs["entrance"].Value.Trim() + ";" +
                        attrs["door"].Value.Trim() + ";" +
                        attrs["total_inside"].Value.Trim() + ";" +
                        attrs["total_outside"].Value.Trim();
                Console.Write("\n"+ line);
                /*
                using (StreamWriter file = new StreamWriter(serverPathTxt, true))
                {
                    file.WriteLine(line);
                }
                */
            }
        }

        static void logMessage(string message)
        {
           // string serverPathTxt = AppDomain.CurrentDomain.BaseDirectory + "App_Data\\TransmissionEvents.csv";
            string serverPathTxt = AppDomain.CurrentDomain.BaseDirectory + "App_Data\\Transmissions.csv";
            string hourBegin = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(serverPathTxt, true))
            {
                file.WriteLine(hourBegin+ "-------------------------"+message);
            }
        }




        static void Main(string[] args)
        {
            SetTimer();
            aTimer.Stop();
            aTimer.Dispose();
            // obtendo os dados iniciais de config            
            string serverPathXML = AppDomain.CurrentDomain.BaseDirectory + "App_Data\\UserConfig.xml";
            string userXML = getXMLFileString(serverPathXML);
            XmlDocument docConfig = new XmlDocument();
            docConfig.LoadXml(userXML);

            XmlNode n = docConfig.GetElementsByTagName("connectionString")[0]; // local connection
            XmlAttributeCollection attrs = n.Attributes;
            string localCon = attrs["connectionString"].Value;
            n = docConfig.GetElementsByTagName("user")[0]; // remote connection
            attrs = n.Attributes;
            // o primeiro user name  e password deve estar registrado no banco da azure
            string username = attrs["name"].Value.Trim();
            string pw = attrs["pass"].Value.Trim();
            n = docConfig.GetElementsByTagName("client")[0]; // remote connection
            attrs = n.Attributes;
            string client = attrs["name"].Value.Trim();
            n = docConfig.GetElementsByTagName("location")[0]; // remote connection
            attrs = n.Attributes;
            string location = attrs["name"].Value.Trim();
            n = docConfig.GetElementsByTagName("remoteServer")[0]; // remote connection
            attrs = n.Attributes;
            string uri = attrs["uri"].Value.Trim();
            n = docConfig.GetElementsByTagName("lastDateTransmitted")[0]; // remote connection
            attrs = n.Attributes;
            string lastTransmission = attrs["date"].Value.Trim();

            SqlConnection localConn = new SqlConnection(localCon);
            localConn.Open();

        LoopX:
            string dateToTransmit = getDateToTransmit(localConn, lastTransmission);
            if (dateToTransmit == "")
            {
                //sem dados no banco a partir da hora em last transmission
                logMessage("nao obteve dados no banco após ultima transmissão em:" + lastTransmission);
                return; // o programa sera chamado de 10 em 10 minutos pelo gerenciador de tarefas
            }
            // enviar hora a hora até falhar a transmissao ou terminar os dados
            DataView dv = getSumsForTheHourOneLinePerCamera(localConn, dateToTransmit);
            if (dv == null)
            {
                //sem dados no banco a partir da hora em last transmission
                logMessage("Erro na obtenção dos dados em :" + dateToTransmit);
                return;
            }
            string trStr = getXMLForTheHourOneLinePerCamera(dateToTransmit, dv, docConfig);


            int statusTr = sendTransmission(uri, username, pw, client, location, trStr);
            if (statusTr == (int)server_status.off)
            {
                logMessage("Erro de Transmissão :" + dateToTransmit);
                return;
            }


            logTransmissions(trStr);

            attrs["date"].Value = dateToTransmit;
            docConfig.Save(serverPathXML);
            lastTransmission = dateToTransmit;
            goto LoopX;
        }




    }
}
