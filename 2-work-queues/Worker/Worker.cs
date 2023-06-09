using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();


/*  durable: true Указывает сохранять сообщения при отключении RABBITMQ-сервера
    Случай с ОТКАЗОМ RABBITMQ ШАГ 1 */
channel.QueueDeclare(queue: "task_queue",
                     durable: true,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);

/*  prefetchCount: 1 Указывает не давать более 1 сообщение воркеру за раз Fair Dispatch*/
channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

Console.WriteLine(" [*] Waiting for messages.");

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");

    int dots = message.Split('.').Length - 1;

    /* Эмуляция ошибки, при которой сообщение не обработано для случая с отказом получателя */
    // if (new Random().Next(1, 10) > 4)
    // {
    //     Console.WriteLine("FAKE ERROR");
    //     throw new Exception("Fake Worker Exception");
    // }

    Thread.Sleep(dots * 1000);

    Console.WriteLine(" [x] Done");


    /*  Убедиться, что сообщение не будет считаться доставленным, пока worker не подтвердит это
        Случай с ОТКАЗОМ ПОЛУЧАТЕЛЯ */
    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
};

channel.BasicConsume(queue: "task_queue",
                     // не считать сообщения обработанными автоматически
                     autoAck: false,
                     consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();