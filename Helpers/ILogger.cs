namespace POC.Helpers;
public interface ILogger{

    void Error(string message);
    void Warning(string message);
    void Info(string message);
    void Debug(string message);
    void Trace(string message);
    void Fatal(string message);
    void Critical(string message);
    void Alert(string message);
     
}