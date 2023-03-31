namespace Kafka
{
    public static class Config
    {
        public const string ConnectionString = "Endpoint=sb://XXX.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXX";
        public const string Certificates = "./cacert.pem";
        public const string KafkaBrokers = "XXX.servicebus.windows.net:9093";
    }
}