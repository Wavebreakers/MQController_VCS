using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using IniParser;
using IniParser.Model;
using IBM.WMQ;
//using IBM.WMQAX;

namespace MQController
{
    class Logger
    {
        private static string path;
        public static Logger instance;
        public static void Init(string logPath)
        {
            instance = new Logger(logPath);
        }
        async private static void AppendFile(string message)
        {
            string file = string.Format("{0}{1}-mfg_mq_client.log", path, DateTime.Now.ToString("yyyyMMdd"));
            System.IO.StreamWriter stream = System.IO.File.AppendText(file);
            stream.WriteLine(string.Format("{0} {1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), message));
            stream.Close();
        }
        public static void Debug(string message)
        {
            AppendFile(string.Format("Debug {0}", message));
        }
        public static void Info(string message)
        {
            AppendFile(string.Format("Info {0}", message));
        }
        public static void Warn(string message)
        {
            AppendFile(string.Format("Warn {0}", message));
        }
        public static void Danger(string message)
        {
            AppendFile(string.Format("Error {0}", message));
        }
        private Logger(string logPath)
        {
            path = logPath;
        }
    }

    class MQClient
    {
        private string PUT_APPLICATION_NAME = "SendMQMessage";

        private string host;
        private string port;
        private string channel;
        private string managerName;
        private string queueName;
        private string replyQueueName;
        private string userId;
        private int waitReplyInterval;
        private bool needReply;
        private bool withApplicationIdData;

        MQQueueManager mqQueueManager = null;
        MQQueue mqQueue = null;
        MQQueue mqReplyQueue = null;

        public MQClient(string _userId, string _queueName, string _replyQueueName, string _managerName, string _channel, string _host, string _port = "1414", string _waitReplyInterval = "15000", string _needReply = "true", string _withApplicationIdData = "true")
        {
            userId = _userId;
            queueName = _queueName;
            replyQueueName = _replyQueueName;
            managerName = _managerName;
            channel = _channel;
            host = _host;
            port = _port;
            waitReplyInterval = int.Parse(_waitReplyInterval);
            if (_needReply == "false")
            {
                needReply = false;
            }
            else
            {
                needReply = true;
            }
            if (_withApplicationIdData == "false")
            {
                withApplicationIdData = false;
            } else
            {
                withApplicationIdData = true;
            }
            MQEnvironment.Hostname = host;
            MQEnvironment.Port = int.Parse(port);
            MQEnvironment.Channel = channel;
        }

        public void Connect()
        {
            mqQueueManager = new MQQueueManager(managerName);
            Logger.Info(string.Format("Connect MQQueueManager[Name={0}][Channel={1}][ConnectionName={2}({3})]", mqQueueManager.Name, channel, host, port));
            mqQueue = mqQueueManager.AccessQueue(queueName, MQC.MQOO_OUTPUT | MQC.MQOO_SET_ALL_CONTEXT);
            Logger.Info(string.Format("Connect MQQueue[Gateway][Name={0}]", mqQueue.Name));
            mqReplyQueue = mqQueueManager.AccessQueue(replyQueueName, MQC.MQOO_INPUT_SHARED);
            Logger.Info(string.Format("Connect MQQueue[Reply][Name={0}]", mqReplyQueue.Name));
        }

        public void Connect(int _putFlags, int _getFlags)
        {
            mqQueueManager = new MQQueueManager(managerName);
            Logger.Info(string.Format("Connect MQQueueManager[Name={0}][Channel={1}][ConnectionName={2}({3})]", mqQueueManager.Name, channel, host, port));
            mqQueue = mqQueueManager.AccessQueue(queueName, _putFlags);
            Logger.Info(string.Format("Connect MQQueue[Gateway][Name={0}]", mqQueue.Name));
            mqReplyQueue = mqQueueManager.AccessQueue(replyQueueName, _getFlags);
            Logger.Info(string.Format("Connect MQQueue[Reply][Name={0}]", mqReplyQueue.Name));
        }

        public void Disconnect()
        {
            if (mqQueue != null)
            {
                Logger.Info(string.Format("Disconnect MQQueue[Gateway][Name={0}]", mqQueue.Name));
                mqQueue.Close();
            }
            if (mqReplyQueue != null)
            {
                Logger.Info(string.Format("Disconnect MQQueue[Reply][Name={0}]", mqReplyQueue.Name));
                mqReplyQueue.Close();
            }
            if (mqQueueManager != null)
            {
                if (mqQueueManager.IsConnected)
                {
                    mqQueueManager.Disconnect();
                }
                Logger.Info(string.Format("Disconnect MQQueueManager[Name={0}]", mqQueueManager.Name));
                mqQueueManager.Close();
            }
        }

        public Dictionary<string, dynamic> SendMessage(string inputMessage, string applicationIdData)
        {
            DateTime ts = DateTime.Now;

            MQMessage request;
            MQMessage response;
            MQPutMessageOptions requestInit;
            MQGetMessageOptions responseInit;
            byte[] messageId;
            Dictionary<string, dynamic> result = new Dictionary<string, dynamic>();
            result.Add("success", false);
            result.Add("message", "");
            try
            {
                messageId = GenerateMessageId(ts);
                request = new MQMessage();
                requestInit = new MQPutMessageOptions();
                requestInit.Options = MQC.MQPMO_SET_ALL_CONTEXT;
                request.UserId = userId;
                request.PutApplicationName = PUT_APPLICATION_NAME;
                request.PutApplicationType = 28; /// 0b11100
                if (withApplicationIdData)
                {
                    request.ApplicationIdData = applicationIdData;
                }
                request.MessageId = messageId;
                request.Format = MQC.MQFMT_STRING;
                request.CharacterSet = 950;
                request.Encoding = 546;
                request.PutDateTime = ts;
                request.ReplyToQueueName = replyQueueName;
                request.WriteString(inputMessage);
                //mqQueue.ClearErrorCodes();
                mqQueue.Put(request, requestInit);

                response = new MQMessage();
                response.MessageId = messageId;
                responseInit = new MQGetMessageOptions();
                responseInit.Options = MQC.MQGMO_FAIL_IF_QUIESCING | MQC.MQGMO_WAIT;
                responseInit.MatchOptions = MQC.MQMO_MATCH_MSG_ID;
                responseInit.WaitInterval = waitReplyInterval; /// default 15000ms

                try
                {
                    //mqReplyQueue.ClearErrorCodes();
                    if (needReply)
                    {
                        mqReplyQueue.Get(response, responseInit);
                    }
                    result["success"] = true;
                }
                catch (Exception error)
                {
                    result["message"] = string.Format("[Error][{0}]{1}", error.Source, error.Message);
                    return result;
                }
                if (needReply)
                {
                    result["message"] = response.ReadString(response.MessageLength);
                }
                else
                {
                    result["message"] = "Successfully Send Message";
                }

            }
            catch (MQException error)
            {
                result["message"] = string.Format("[Error][{0}]{1}", error.Source, error.Message);
                return result;
            }
            catch (Exception error)
            {
                result["message"] = string.Format("[Error][{0}]{1}", error.Source, error.Message);
                return result;
            }

            return result;
        }

        private static byte[] GenerateMessageId(DateTime ts)
        {
            string id = "";
            Random r = new Random(ts.Second);
            id += ts.ToString("yyyyMMddHHmmss");
            id += r.Next(10000).ToString();

            return System.Text.Encoding.ASCII.GetBytes(id);
        }
    }

    class Program
    {
        static void DisplayDocument()
        {
            Console.WriteLine("Usage: SendMQMessage <applicationIdData> <message> [...more_message]");
            Console.WriteLine("  Env: MFG_MQ_INI (default: config.ini) -> Path to ini file");
        }

        [STAThread]
        static void Main(string[] args)
        {
            if (args.Length > 0 && (args[0] == "-h" || args[0] == "--help"))
            {
                DisplayDocument();
                return;
            }
            FileIniDataParser iniLoader = new FileIniDataParser();
            IniData configure;
            string currentPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            string iniPath = System.IO.Path.Combine(currentPath, "SendMQMessage.ini");
            string applicationIdData = null;
            string[] inputMessages = null;
            List<Dictionary<string, string>> inputMessageFiles = null;
            bool flag = false; /// true to read messages from resource directory

            if (System.Environment.GetEnvironmentVariable("MFG_MQ_INI") != null)
            {
                iniPath = System.Environment.GetEnvironmentVariable("MFG_MQ_INI");
            }
            
            try
            {
                configure = iniLoader.ReadFile(iniPath);
                Logger.Init(configure["PATH"]["APP_LOG_PATH"]);
                /// mode: cli | file
                /// cli -> read messages from args[]
                /// file -> read messages from files in resource directory
                /// prepare messages
                if (configure["APP"]["MODE"] == "cli" || args.Length >= 2)
                {
                    /// check argv >= 2
                    if (args.Length < 2)
                    {
                        DisplayDocument();
                        return;
                    }
                    applicationIdData = args[0];
                    inputMessages = new string[args.Length - 1];
                    
                    for (int i = 1; i < args.Length; i ++)
                    {
                        inputMessages[i - 1] = args[i];
                    }
                }
                else if (configure["APP"]["MODE"] == "file")
                {
                    /// read resource directory
                    Regex validTest = new Regex("\\[(.+)\\](.+)\\.trx$"); ///* /\[(.+)\](.+)\.trx$/

                    inputMessageFiles = new List<Dictionary<string, string>>();

                    foreach (string filename in System.IO.Directory.GetFiles(configure["PATH"]["RESOURCE_PATH"]).Where((string filename) => validTest.IsMatch(filename)))
                    {
                        string _applicationIdData;
                        string content = System.IO.File.ReadAllText(filename);
                        if (content.Length == 0)
                        {
                            continue;
                        }
                        _applicationIdData = validTest.Match(filename).Groups[1].ToString();

                        Dictionary<string, string> entity = new Dictionary<string, string>();
                        entity.Add("filename", filename);
                        entity.Add("content", content);
                        entity.Add("applicationIdData", _applicationIdData);
                        inputMessageFiles.Add(entity);
                    }
                    if (inputMessageFiles.ToArray().Length == 0)
                    {
                        Console.WriteLine("[Warn]No resource files found.");
                        Logger.Info("No resource files found.");
                        return;
                    }
                    
                    flag = true;

                }
                else
                {
                    throw new Exception(string.Format("Unknow mode set: {0}; Expected: cli | file", configure["APP"]["MODE"]));
                }
            }
            catch (Exception error)
            {
                Logger.Danger(error.Message);
                Console.WriteLine(error.Message);
                return;
            }

            MQClient client;

            client = new MQClient(configure["MQ"]["USER_ID"], configure["MQ"]["QUEUE_NAME"], configure["MQ"]["REPLY_QUEUE_NAME"], configure["MQ"]["MANAGER_NAME"], configure["MQ"]["CHANNEL"], configure["MQ"]["HOST"], configure["MQ"]["PORT"], configure["MQ"]["WAIT_REPLY_INTERVAL"], configure["MQ"]["NEED_REPLY"], configure["MQ"]["WITH_APPLICATION_ID_DATA"]);

            try
            {
                client.Connect();
                /// send messages
                if (flag)
                {
                    foreach (Dictionary<string, string> entity in inputMessageFiles)
                    {
                        /// move .trx to backup directory after success send message
                        /// if backup directory is invalid, delete it.
                        Dictionary<string, dynamic> result = client.SendMessage(entity["content"], entity["applicationIdData"]);
                        if ((bool)result["success"])
                        {
                            Console.WriteLine(string.Format("success\n->[{0}]{1}\n<-{2}", entity["applicationIdData"], entity["content"], result["message"]));
                            Logger.Info(string.Format("->[{0}]{1}\n<-{2}", entity["applicationIdData"], entity["content"], result["message"]));

                            try
                            {
                                System.IO.File.Move(entity["filename"], System.IO.Path.Combine(configure["PATH"]["RESOURCE_BACKUP_PATH"], System.IO.Path.GetFileName(entity["filename"])));
                                Logger.Info(string.Format("Move trx to backup"));
                            }
                            catch (Exception error)
                            {
                                Console.WriteLine("Move to backup failed, {0}", error.Message);
                                Logger.Warn(string.Format("Move trx to backup failed, {0}", error.Message));
                                System.IO.File.Delete(entity["filename"]);
                            }
                        }
                        else
                        {
                            Console.WriteLine(string.Format("failure\n->[{0}]{1}\n<-{2}", entity["applicationIdData"], entity["content"], result["message"]));
                            Logger.Warn(string.Format("->[{0}]{1}\n<-{2}", entity["applicationIdData"], entity["content"], result["message"]));
                            System.IO.File.Delete(entity["filename"]);
                        }
                        System.Threading.Thread.Sleep(300); /// wait 300 ms to send next message
                    }
                }
                else
                {
                    foreach (string message in inputMessages)
                    {
                        Dictionary<string, dynamic> result = client.SendMessage(message, applicationIdData);
                        if ((bool)result["success"])
                        {
                            Console.WriteLine(string.Format("success\n->[{0}]{1}\n<-{2}", applicationIdData, message, result["message"]));
                            Logger.Info(string.Format("->[{0}]{1}\n<-{2}", applicationIdData, message, result["message"]));
                        }
                        else
                        {
                            Console.WriteLine(string.Format("failure\n->[{0}]{1}\n<-{2}", applicationIdData, message, result["message"]));
                            Logger.Warn(string.Format("->[{0}]{1}\n<-{2}", applicationIdData, message, result["message"]));
                        }
                        System.Threading.Thread.Sleep(300); /// wait 300 ms to send next message
                    }
                }
            }
            catch (MQException error)
            {
                Logger.Danger(error.Message);
                Console.WriteLine(error.Message);
                return;
            }
            catch (Exception error)
            {
                Logger.Danger(error.Message);
                Console.WriteLine(error.Message);
                return;
            }
            finally
            {
                client.Disconnect();
            }
        }
    }
}
