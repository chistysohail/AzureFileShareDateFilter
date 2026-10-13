using Azure;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Microsoft.Extensions.Configuration;
using System.Globalization;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var settings = configuration.GetSection("AzureFileShare").Get<FileShareSettings>()
    ?? throw new InvalidOperationException("Missing AzureFileShare settings.");

var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING")
    ?? settings.ConnectionString;
if (string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(settings.ShareName))
    throw new InvalidOperationException("Set the storage connection string and share name.");

var start = DateOnly.ParseExact(settings.StartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
var end = DateOnly.ParseExact(settings.EndDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
if (end < start) throw new ArgumentException("EndDate must be on or after StartDate.");
var fromUtc = new DateTimeOffset(start.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
var untilUtcExclusive = new DateTimeOffset(end.AddDays(1).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);

var share = new ShareClient(connectionString, settings.ShareName);
var root = share.GetRootDirectoryClient();
var directory = root;
var basePath = settings.DirectoryPath.Trim('/');
foreach (var segment in basePath.Split('/', StringSplitOptions.RemoveEmptyEntries))
    directory = directory.GetSubdirectoryClient(segment);

Console.WriteLine($"Share: {settings.ShareName}; UTC modified date: {start} through {end}");
int count = 0;
await ScanAsync(directory, basePath);
Console.WriteLine($"Total matching files: {count}");

async Task ScanAsync(ShareDirectoryClient current, string path)
{
    try
    {
        await foreach (ShareFileItem item in current.GetFilesAndDirectoriesAsync())
        {
            var childPath = string.IsNullOrEmpty(path) ? item.Name : $"{path}/{item.Name}";
            if (item.IsDirectory)
            {
                if (settings.Recursive)
                    await ScanAsync(current.GetSubdirectoryClient(item.Name), childPath);
                continue;
            }

            try
            {
                var file = current.GetFileClient(item.Name);
                var properties = (await file.GetPropertiesAsync()).Value;
                var modified = properties.LastModified;
                if (modified >= fromUtc && modified < untilUtcExclusive)
                {
                    count++;
                    Console.WriteLine($"{modified:yyyy-MM-dd HH:mm:ss} UTC | {properties.ContentLength,12} bytes | {childPath}");
                }
            }
            catch (RequestFailedException ex)
            {
                Console.Error.WriteLine($"Could not inspect {childPath}: {ex.Message}");
            }
        }
    }
    catch (RequestFailedException ex)
    {
        Console.Error.WriteLine($"Could not list directory '{path}': {ex.Message}");
    }
}

sealed class FileShareSettings
{
    public string ConnectionString { get; set; } = "";
    public string ShareName { get; set; } = "";
    public string DirectoryPath { get; set; } = "";
    public string StartDate { get; set; } = "";
    public string EndDate { get; set; } = "";
    public bool Recursive { get; set; } = true;
}

