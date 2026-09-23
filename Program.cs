using POC.Helpers;
using Pollers;
using Microsoft.Extensions.Configuration;

var localConfig = new EnvironmentConfig(new ConfigurationBuilder().AddJsonFile("appsettings.json").Build());
var loggerFactory = new PollerLoggerFactory(localConfig);

var poller1 = new Poller1(loggerFactory.Create<Poller1>());
var poller2 = new Poller2(loggerFactory.Create<Poller2>());

poller1.ExecutePoller();
poller2.ExecutePoller();

Console.WriteLine("Poller1 and Poller2 started");
Console.ReadLine();
