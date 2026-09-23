using System;
using POC.Helpers;
namespace Pollers
{
    public class Poller1
    {
        private readonly ILogger _logger;
        public Poller1(ILogger logger)
        {
            _logger = logger;
        }
        public void ExecutePoller()
        {
            var token = new CancellationTokenSource();
            ThreadStart threadStart = new ThreadStart(() =>
            {
                while (!token.IsCancellationRequested)
                {
                    _logger.Info("Poller1 is polling");
                    Thread.Sleep(3000);
                }
            });
            Thread thread = new Thread(threadStart);
            thread.Start();
            _logger.Info("Poller1 started");
        }
    }
}