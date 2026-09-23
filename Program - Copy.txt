using POC.Helpers;
using Pollers;
using NLog;
using Microsoft.Extensions.Configuration;

var logConfig = new NLog.Config.LoggingConfiguration();
var localConfig = new EnvironmentConfig(new ConfigurationBuilder().AddJsonFile("appsettings.json").Build());

// setup our console logger
var consoleLogger = new NLog.Targets.ConsoleTarget(localConfig.GetValue("NLog:targets:console:type"));
consoleLogger.Layout = localConfig.GetValue("NLog:targets:console:layout");

var fileLayout = localConfig.GetValue("NLog:targets:logfile:layout");
var fileNameTemplate = localConfig.GetValue("NLog:targets:logfile:fileName");

// setup per-poller file loggers
var poller1FileLogger = new NLog.Targets.FileTarget("Poller1File");
poller1FileLogger.FileName = string.Format(fileNameTemplate, "Poller1");
poller1FileLogger.Layout = fileLayout;

var poller2FileLogger = new NLog.Targets.FileTarget("Poller2File");
poller2FileLogger.FileName = string.Format(fileNameTemplate, "Poller2");
poller2FileLogger.Layout = fileLayout;

// configure the outputs
logConfig.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Fatal, consoleLogger);
logConfig.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Fatal, poller1FileLogger, "Poller1");
logConfig.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Fatal, poller2FileLogger, "Poller2");

NLog.LogManager.Configuration = logConfig;

// pass a dedicated logger into each poller
var logger1 = new POC.Helpers.Logger(NLog.LogManager.GetLogger("Poller1"));
var logger2 = new POC.Helpers.Logger(NLog.LogManager.GetLogger("Poller2"));
var poller1 = new Poller1(logger1);
var poller2 = new Poller2(logger2);
poller1.ExecutePoller();
poller2.ExecutePoller();
Console.WriteLine("Poller1 and Poller2 started");
Console.ReadLine();
