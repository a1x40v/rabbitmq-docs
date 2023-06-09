using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

/*
    Exchange типа fanout рассылает сообщение во все queue, которые знает.
    (т.е. которые привязаны к нему - binding)
*/
channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

var message = GetMessage(args);
var body = Encoding.UTF8.GetBytes(message);

/*  В отличие от предыдущих примеров, не указываем имя конкретной queue */
channel.BasicPublish(exchange: "logs",
                     routingKey: string.Empty,
                     basicProperties: null,
                     body: body);

Console.WriteLine($" [x] Sent {message}");

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();

static string GetMessage(string[] args)
{
    return ((args.Length > 0) ? string.Join(" ", args) : "info: Hello World!");
}