using NLog;
using NLog.Config;
using NLog.Targets;

namespace POC.Helpers;

public class PollerLoggerFactory
{
    private readonly LoggingConfiguration _logConfig;
    private readonly string _fileNameTemplate;
    private readonly string _fileLayout;
    private readonly HashSet<string> _registeredPollers = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _sync = new();

    public PollerLoggerFactory(EnvironmentConfig config)
    {
        _logConfig = new LoggingConfiguration();
        _fileNameTemplate = config.GetValue("NLog:targets:logfile:fileName");
        _fileLayout = config.GetValue("NLog:targets:logfile:layout");

        var consoleTarget = new ConsoleTarget(config.GetValue("NLog:targets:console:type"))
        {
            Layout = config.GetValue("NLog:targets:console:layout")
        };
        _logConfig.AddRule(LogLevel.Info, LogLevel.Fatal, consoleTarget);

        LogManager.Configuration = _logConfig;
    }

    public ILogger Create<TPoller>() where TPoller : class
    {
        var pollerName = typeof(TPoller).Name;

        lock (_sync)
        {
            if (_registeredPollers.Add(pollerName))
            {
                var fileTarget = new FileTarget($"{pollerName}File")
                {
                    FileName = string.Format(_fileNameTemplate, pollerName),
                    Layout = _fileLayout
                };

                _logConfig.AddRule(LogLevel.Info, LogLevel.Fatal, fileTarget, pollerName);
                LogManager.Configuration = _logConfig;
            }
        }

        return new Logger(LogManager.GetLogger(pollerName));
    }
}
