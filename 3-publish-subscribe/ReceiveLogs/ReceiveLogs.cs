using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

/* В отличие от предыдущих примеров, не указываем имя конкретной queue

Firstly, whenever we connect to Rabbit we need a fresh, empty queue with generated name.
Secondly, once we disconnect the consumer the queue should be automatically deleted.

In the .NET client, when we supply no parameters to QueueDeclare() we create a non-durable, 
exclusive, autodelete queue with a generated name
*/
var queueName = channel.QueueDeclare().QueueName;


/*  Указываем queue принимать сообщения от указанного exchange (binding) */
channel.QueueBind(queue: queueName,
                  exchange: "logs",
                  routingKey: string.Empty);

Console.WriteLine(" [*] Waiting for logs.");

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] {message}");
};

channel.BasicConsume(queue: queueName,
                     autoAck: true,
                     consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();