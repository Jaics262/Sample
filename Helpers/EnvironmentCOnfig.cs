using System.Reflection;
using Microsoft.Extensions.Configuration;

public class EnvironmentConfig
{
    private readonly IConfiguration config;
    public EnvironmentConfig(IConfiguration config)
    {
        this.config = config;
    }
    public string GetValue(string key)
    {
        return this.config.GetSection(key).Value ?? string.Empty;
    }
}