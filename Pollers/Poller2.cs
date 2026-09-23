using System;
using POC.Helpers;
namespace Pollers{
    public class Poller2{
        private readonly ILogger _logger;
    public      Poller2(ILogger logger){
        _logger = logger;
    }
        public void ExecutePoller(){
             var token = new CancellationTokenSource();
             ThreadStart threadStart = new ThreadStart(() => {
                while(!token.IsCancellationRequested){
                    _logger.Info("Poller2 is polling");
                    Thread.Sleep(3000);
                }
             });
             Thread thread = new Thread(threadStart);
             thread.Start();
             _logger.Info("Poller2 started");
        }
    }
}