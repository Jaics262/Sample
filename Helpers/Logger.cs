namespace POC.Helpers;

public class Logger : ILogger
{
    private readonly NLog.ILogger _logger;

    public Logger(NLog.ILogger logger)
    {
        _logger = logger;
    }

    public void Error(string message) => _logger.Error(message);
    public void Warning(string message) => _logger.Warn(message);
    public void Info(string message) => _logger.Info(message);
    public void Debug(string message) => _logger.Debug(message);
    public void Trace(string message) => _logger.Trace(message);
    public void Fatal(string message) => _logger.Fatal(message);


}
