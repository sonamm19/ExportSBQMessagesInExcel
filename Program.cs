using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus;
using OfficeOpenXml;
using System.Text;
using System.Xml.Linq;


class Program
{
    const string ServiceBusConnectionString = "";
    const string QueueNameDev = "";

    static async Task Main(string[] args)
    {
        // Create a ServiceBusClient
        ServiceBusClient client = new ServiceBusClient(ServiceBusConnectionString);

        // Create a receiver for the dead letter queue
        string deadLetterQueueName = QueueNameDev + "/$DeadLetterQueue";
        ServiceBusReceiver receiver = client.CreateReceiver(deadLetterQueueName);
        List<string> messageIds = new List<string>();
        try
        {
            int totalMessages = 200; // Total number of messages to receive
            int receivedMessagesCount = 0;
            List<Message> messagesToExport = new List<Message>();

            while (receivedMessagesCount < totalMessages)
            {
                // Peek messages from the dead letter queue in batches
                int maxMessages = Math.Min(50, totalMessages - receivedMessagesCount); // Adjust batch size if necessary
                IReadOnlyList<ServiceBusReceivedMessage> messages = await receiver.PeekMessagesAsync(maxMessages);

                if (messages.Count == 0)
                {
                    break; // No more messages to process
                }

                foreach (ServiceBusReceivedMessage message in messages)
                {

                    var legacyMessage = ConvertToLegacyMessage(message);
                    messagesToExport.Add(legacyMessage);
                    receivedMessagesCount++;

                    if (receivedMessagesCount >= totalMessages)
                    {
                        break; // Reached the total messages limit
                    }
                }
            }

            Console.WriteLine($"Received Messages Count: {receivedMessagesCount}");

            // Export messages to Excel after processing all messages
            ExportMessagesToExcel(messagesToExport);

            Console.WriteLine($"Processed total: {messagesToExport.Count}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error occurred: {ex.Message}");
        }
        finally
        {
            // Dispose the receiver
            await receiver.DisposeAsync();
            // Dispose the client
            await client.DisposeAsync();
        }
    }

    static void ExportMessagesToExcel(List<Message> messages)
    {
        string currentDirectory = "";
        string currentDirectory2 = AppDomain.CurrentDomain.BaseDirectory;
        string filePath = Path.Combine(currentDirectory, "DeadLetterMessages.xlsx");

        // Ensure EPPlus is properly licensed
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
        int row = 0;
        using (var package = new ExcelPackage())
        {
            var worksheet = package.Workbook.Worksheets.Add("DeadLetterMessages");
            worksheet.Cells[1, 1].Value = "MessageId";
            worksheet.Cells[1, 2].Value = "Body";
            worksheet.Cells[1, 3].Value = "EnqueuedTime";
            worksheet.Cells[1, 4].Value = "PayloadType";
            worksheet.Cells[1, 5].Value = "Code";
            worksheet.Cells[1, 6].Value = "DeadLetterReason";

            row = 2;

            foreach (var message in messages)
            {
                string xmlString = Encoding.UTF8.GetString(message.Body);

                worksheet.Cells[row, 1].Value = message.MessageId;
                worksheet.Cells[row, 2].Value = xmlString;
                worksheet.Cells[row, 3].Value = ConvertUtcToNzTime(message.ScheduledEnqueueTimeUtc);

                if (message.UserProperties.TryGetValue("PayloadType", out object payloadType) != null)
                {
                    worksheet.Cells[row, 4].Value = payloadType.ToString();
                }
                if (payloadType.ToString().Contains("Compound"))
                {
                    XDocument doc = XDocument.Parse(xmlString);
                    XElement employeeNumberElement = doc.Descendants("EmployeeNumber").FirstOrDefault();
                    if(employeeNumberElement != null)
                    {
                        string employeeNumber = employeeNumberElement.Value;
                        worksheet.Cells[row, 5].Value = employeeNumber?.ToString();
                    }
                }
                else
                {
                    XDocument doc = XDocument.Parse(xmlString);
                    XElement positionElement = doc.Descendants("PositionCode").FirstOrDefault();
                    if (positionElement != null)
                    {
                        string pdtCode = positionElement.Value;
                        worksheet.Cells[row, 5].Value = pdtCode?.ToString();
                    }
                }

                if (message.UserProperties.TryGetValue("ErrorMessage", out object ErrorMessage) != null)
                {
                    worksheet.Cells[row, 6].Value = ErrorMessage?.ToString();
                }

                row++;
            }

            // Save the Excel file
            var fileInfo = new FileInfo(filePath);
            package.SaveAs(fileInfo);

            Console.WriteLine($"Messages exported to Excel successfully. {row}");
        }
    }

    static Message ConvertToLegacyMessage(ServiceBusReceivedMessage newMessage)
    {
        var legacyMessage = new Microsoft.Azure.ServiceBus.Message(newMessage.Body.ToArray())
        {
            MessageId = newMessage.MessageId,
            ContentType = newMessage.ContentType,
            CorrelationId = newMessage.CorrelationId,
            Label = newMessage.Subject,
            PartitionKey = newMessage.PartitionKey,
            ReplyTo = newMessage.ReplyTo,
            ReplyToSessionId = newMessage.ReplyToSessionId,
            ScheduledEnqueueTimeUtc = newMessage.EnqueuedTime.UtcDateTime,
            SessionId = newMessage.SessionId,
            TimeToLive = newMessage.TimeToLive,
            To = newMessage.To,
        };

        // Copy user properties
        foreach (var property in newMessage.ApplicationProperties)
        {
            legacyMessage.UserProperties.Add(property.Key, property.Value);
        }

        return legacyMessage;
    }

    static string ConvertUtcToNzTime(DateTime utcDateTime)
    {
        // Define New Zealand time zone
        TimeZoneInfo nzTimeZone = TimeZoneInfo.FindSystemTimeZoneById("New Zealand Standard Time"); // Adjust as per actual timezone

        // Convert UTC to NZ time
        DateTime nzDateTime = TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, nzTimeZone);

        string nzDateTimeFormatted = nzDateTime.ToString("dd/MM/yyyy hh:mm:ss tt");
        return nzDateTimeFormatted;
    }
}